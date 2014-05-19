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
 * @author changun
 * BaseLifestreamsStatefulBolt is the base class of all the Lifestreams components.
 * It assume that the received tuples is grouped the OhmageUser.
 * 
 * It provides the following functionality:
 *  
 * 1. Writeback: If the target ohmage stream is specified and dryrun == false, the 
 *    processed records will be upload to the specified ohmage stream.
 *    
 * 2. COMMAND Receiving and propagation (EXPERIMENTAL): perform the specified operation when 
 *    receiving a command, and propagate the command if the command targets one of 
 *    its descendant components.
 */
public class LifestreamsBolt extends BaseRichBolt implements
		IGenerator {
	protected static final String UNAKCED_TUPLES_FIELD_NAME = "_UnakcedTuples";

	
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

	protected HashMap<OhmageUser, UserState> userStateMap = new HashMap<OhmageUser, UserState>();
	protected HashMap<OhmageUser, UserState> finishedUserStateMap = new HashMap<OhmageUser, UserState>();
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
	
	private UserState createUserState(OhmageUser user, Long batchId){
		UserState userState;
		userStateMap.put(user, UserState.createOrRecoverUserState(user, templateTask, batchId, this, bookkeeper, serializer));
		
		userState = userStateMap.get(user);
		return userState;
	}
	private UserState getUserState(OhmageUser user){
		return userStateMap.get(user);
	}
	private void removeUserState(OhmageUser user){
		userStateMap.remove(user);
	}
	@Override
	public void execute(Tuple input) {
		BaseTuple baseTuple = BaseTuple.createFromRawTuple(input);
		OhmageUser user = baseTuple.getUser();
		
		if(baseTuple instanceof StreamStatusTuple){
			StreamStatusTuple tuple = (StreamStatusTuple)baseTuple;
			switch(tuple.getStatus()){
				case HEAD:
					createUserState(user, tuple.getBatchId());
					break;
				case END:
					UserState userState = getUserState(user);
					if(userState!=null){
						userState.streamEnd();
						removeUserState(user);
					}
					break;
				case MIDDLE:
					break;
			}
			collector.emit(baseTuple.getTuple(), baseTuple.getValues());
			return;
		}
		UserState userState = getUserState(user);
		if(userState == null){
			// the user state has not been created, ignore the tuple
			return;
		}else if(baseTuple instanceof RecordTuple){
			userState.execute((RecordTuple) baseTuple);
		}else if(baseTuple instanceof GlobalCheckpointTuple){
			userState.executeGlobalCheckpoint((GlobalCheckpointTuple) baseTuple);
		}
	}

	protected void upload(StreamRecord<? extends Object> dp) {
		try {

			new OhmageStreamClient(dp.getUser()).upload(targetStream,
					dp.toObserverDataPoint());
		} catch (OhmageAuthenticationError e) {
			throw new  RuntimeException(e);
		} catch (IOException e) {
			throw new  RuntimeException(e);
		}
	}

	public void emitRecord(StreamRecord<? extends Object> rec, Long batchId, List<Tuple> anchors, boolean upload) {
		if(upload && !isDryrun && targetStream != null){
			// upload the processed record back to ohmage
			streamStore.upload(targetStream, rec);
		}
		// emit a new Tuple, anchoring all the previous tuples
		if(this.subComponents.size() > 0){
			collector.emit(anchors, new RecordTuple(rec, batchId).getValues());
		}
	}
	public void emit(BaseTuple tuple, List<Tuple> anchors) {
		// emit a new Tuple, anchoring all the previous tuples
		if(this.subComponents.size() > 0){
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
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		serializer = new SerializationFactory().getKryo(stormConf);
		// initialize the topology-wide arguments
		if(stormConf.containsKey(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING)){
			isDryrun = (Boolean) stormConf.get(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING);
		}
		this.collector = collector;
		// populate the names of this bolt and the topology
		this.componentId = context.getThisComponentId();
		this.topologyId = context.getStormId();
		// get the source spouts of this component
		this.sourceIds = new HashSet<String>();
		for(String spout: context.getRawTopology().get_spouts().keySet()){
			Set<String> nodes = new HashSet<String>();
			if(this.getSubGraph(context, spout, nodes).contains(this.componentId)){
				sourceIds.add(spout);
			}
		}
		// get the descendant of this bolt
		subComponents = new HashSet<String>();
		getSubGraph(context, this.getComponentId(), subComponents);
		// get the input streams of this bolt
		inputStreams = new HashSet<GlobalStreamId>();
		for (GlobalStreamId streamId : context.getSources(this.getComponentId()).keySet()) {
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
	 * if the target stream is set and dryrun = false, the processed 
	 * records will be write back to the given stream stream. 
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
