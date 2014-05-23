package org.ohmage.lifestreams.bolts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.LifestreamsConfig;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.spouts.IBookkeeper;
import org.ohmage.lifestreams.stores.StreamStore;
import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.lifestreams.tuples.BaseTuple;
import org.ohmage.lifestreams.tuples.GlobalCheckpointTuple;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.lifestreams.tuples.StreamStatusTuple;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.models.OhmageUser.OhmageAuthenticationError;
import org.ohmage.sdk.OhmageStreamClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * LifestreamsBolt is a implementation of Storm Bolt. Each Lifestreams Bolt is
 * assigned a Task that contains the main data processing logic. However,
 * instead of directly interacting with the Task object, Lifestreams bolt
 * creates a {@link UserTaskState} instance for each user. {@link UserTaskState}
 * maintains the computation states and cache the outputs. Whenever the task
 * commits a checkpoint, the UserState object will make a snapshot of the
 * computation state and store it in a persistent storage, so that the
 * LifestreamsBolt can recover/resume the computation state after crash/failure.
 * LifestreamsBolt provides several function for {@link UserTaskState} to
 * interact with storm topology. For example,
 * {@link #emitRecord(StreamRecord, List, boolean)} is called to emit a stream
 * record to the topology and optionally upload the record to the stream store
 * (if specified).
 * 
 * @author changun
 * 
 */
public class LifestreamsBolt extends BaseRichBolt implements IGenerator {
	// UserTaskState for each user
	private HashMap<OhmageUser, UserTaskState> userStateMap = new HashMap<OhmageUser, UserTaskState>();

	// storm data collector (for emitting records)
	protected OutputCollector collector;

	// the name of the topology, and the name of this bolt
	// these values will be populated during prepare phase (see prepare())
	private String topologyId;
	private String componentId;
	private Set<String> sourceIds;
	// the components in the subtree with this bolt as root.
	// (when a bolt receives a command targeting one of its subcomponent,
	// it should propagate the command)
	Set<String> subComponents;

	// the input streams of this bolt
	Set<GlobalStreamId> inputStreams;

	// logger
	Logger logger = LoggerFactory.getLogger(LifestreamsBolt.class);

	// whether to write back the processed records to the ohmage stream.
	private boolean isDryrun = false;
	// the stream store, where the processed data being output to
	private StreamStore streamStore;
	// the ohmage stream the processed records will be writeback to.
	private OhmageStream targetStream;

	protected Kryo serializer;
	protected IBookkeeper bookkeeper;
	final private Task templateTask;

	private UserTaskState createUserState(OhmageUser user, Long batchId) {
		UserTaskState userState;
		userStateMap.put(user, UserTaskState.createOrRecoverUserState(user,
				templateTask, this, bookkeeper, serializer));

		userState = userStateMap.get(user);
		return userState;
	}

	private UserTaskState getUserState(OhmageUser user) {
		return userStateMap.get(user);
	}

	private void removeUserState(OhmageUser user) {
		userStateMap.remove(user);
	}

	@Override
	public void execute(Tuple input) {
		BaseTuple baseTuple = BaseTuple.createFromRawTuple(input);
		OhmageUser user = baseTuple.getUser();

		if (baseTuple instanceof StreamStatusTuple) {
			StreamStatusTuple tuple = (StreamStatusTuple) baseTuple;
			switch (tuple.getStatus()) {
			case HEAD:
				createUserState(user, tuple.getBatchId());
				break;
			case END:
				UserTaskState userState = getUserState(user);
				if (userState != null) {
					userState.streamEnd();
				}
				break;
			case MIDDLE:
				break;
			}
			collector.emit(baseTuple.getTuple(), baseTuple.getValues());
			return;
		}
		UserTaskState userState = getUserState(user);
		
		if (userState != null) {
			
			if (baseTuple instanceof GlobalCheckpointTuple) {
				userState.executeGlobalCheckpoint((GlobalCheckpointTuple) baseTuple);
			} else if (baseTuple instanceof RecordTuple && !userState.isEnded()) {
				userState.execute((RecordTuple) baseTuple);
			}
		}
	}

	private void upload(StreamRecord<? extends Object> dp) {
		try {

			new OhmageStreamClient(dp.getUser()).upload(targetStream,
					dp.toObserverDataPoint());
		} catch (OhmageAuthenticationError e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Emit a stream record tuple
	 * 
	 * @param rec
	 *            Record to be emitted
	 * @param anchors
	 *            The input tuple this record depends on
	 * @param upload
	 *            Whether, in addition to emitting it, to upload the tuple to
	 *            the defined stream store.
	 */
	public void emitRecord(StreamRecord<? extends Object> rec,
			List<Tuple> anchors, boolean upload) {
		if (upload && !isDryrun && targetStream != null) {
			// upload the processed record back to ohmage
			streamStore.upload(targetStream, rec);
		}
		// emit a new Tuple, anchoring the input tuples
		emit(new RecordTuple(rec), anchors);
	}

	/**
	 * Emit a tuple to the next bolts.
	 * 
	 * @param tuple
	 * @param anchors
	 */
	public void emit(BaseTuple tuple, List<Tuple> anchors) {
		// emit a new Tuple, anchoring all the previous tuples
		if (this.subComponents.size() > 0) {
			collector.emit(anchors, tuple.getValues());
		}
	}

	// DFS to get all the descendants of this Bold
	private Set<String> getSubGraph(TopologyContext context, String rootId,
			Set<String> ret) {
		ret.add(rootId);
		for (Map<String, Grouping> child : context.getTargets(rootId).values()) {
			for (String childComponentId : child.keySet()) {
				getSubGraph(context, childComponentId, ret);
			}
		}
		return ret;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(BaseTuple.getFields());
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {
		serializer = new SerializationFactory().getKryo(stormConf);
		// initialize the topology-wide arguments
		if (stormConf.containsKey(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING)) {
			isDryrun = (Boolean) stormConf
					.get(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING);
		}
		this.collector = collector;
		// populate the names of this bolt and the topology
		this.componentId = context.getThisComponentId();
		this.topologyId = context.getStormId();
		// get the source spouts of this component
		this.sourceIds = new HashSet<String>();
		for (String spout : context.getRawTopology().get_spouts().keySet()) {
			Set<String> nodes = new HashSet<String>();
			if (this.getSubGraph(context, spout, nodes).contains(
					this.componentId)) {
				sourceIds.add(spout);
			}
		}
		// get the descendant of this bolt
		subComponents = new HashSet<String>();
		getSubGraph(context, this.getComponentId(), subComponents);
		// get the input streams of this bolt
		inputStreams = new HashSet<GlobalStreamId>();
		for (GlobalStreamId streamId : context
				.getSources(this.getComponentId()).keySet()) {
			inputStreams.add(streamId);
		}
	}

	public StreamStore getStreamStore() {
		return streamStore;
	}

	public void setStreamStore(StreamStore streamStore) {
		this.streamStore = streamStore;
	}

	/**
	 * if the target stream is set and dryrun = false, the processed records
	 * will be write back to the given stream stream.
	 */
	public OhmageStream getTargetStream() {
		return targetStream;
	}

	public void setTargetStream(OhmageStream targetStream) {
		this.targetStream = targetStream;
	}

	public Set<GlobalStreamId> getInputStreams() {
		return inputStreams;
	}

	public String getComponentId() {
		return componentId;
	}

	@Override
	public String getGeneratorId() {
		return getComponentId();
	}

	@Override
	public Set<String> getSourceIds() {
		return this.sourceIds;
	}

	@Override
	public String getTopologyId() {
		return topologyId;
	}

	public LifestreamsBolt(Task templateTask, IBookkeeper bookkeeper) {
		this.bookkeeper = bookkeeper;
		this.templateTask = templateTask;
	}

}
