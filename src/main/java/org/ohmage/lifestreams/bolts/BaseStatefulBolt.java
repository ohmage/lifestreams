package org.ohmage.lifestreams.bolts;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ohmage.lifestreams.LifestreamsConfig;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.state.UserState;
import org.ohmage.lifestreams.stores.StreamStore;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.models.OhmageUser.OhmageAuthenticationError;
import org.ohmage.sdk.OhmageStreamClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
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
public abstract class BaseStatefulBolt extends BaseRichBolt implements
		IGenerator {

	
	private static final String UNAKCED_TUPLES_FIELD_NAME = "_UnakcedTuples";

	
	// storm data collector (for emitting records)
	private OutputCollector collector;

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

	private HashMap<OhmageUser, UserState> userStateMap = new HashMap<OhmageUser, UserState>();

	// called when this bolt receive the data of a new user
	abstract protected void newUser(OhmageUser newUser, UserState state);

	// the operation to perform on each incoming record.
	@SuppressWarnings("rawtypes")
	abstract protected void execute(OhmageUser user, Tuple input,
			UserState state,GlobalStreamId source);

	// the operation to perform on each incoming command
	abstract protected boolean executeCommand(OhmageUser user, Command command, UserState state);
	
	// logger
	Logger logger = LoggerFactory.getLogger(BaseStatefulBolt.class);

	// whether to write back the processed records to the ohmage stream.
	private boolean isDryrun = false; 
	// the stream store, where the processed data being output to
	private StreamStore streamStore;
	// the ohmage stream the processed records will be writeback to.
	private OhmageStream targetStream;

	

	@Override
	public void execute(Tuple input) {
		// get the user
		OhmageUser user = (OhmageUser) input.getValueByField("user");
		UserState userState;
		// check if it is a new user
		if (!userStateMap.containsKey(user)) {
			userStateMap.put(user, new UserState());
			userState = userStateMap.get(user);
			userState.put(UNAKCED_TUPLES_FIELD_NAME, new LinkedList<Tuple>());
			// new user method should implement the necessary initialization for
			// the user state
			newUser(user, userState);
		} else {
			// else, get the state for this user
			userState = userStateMap.get(user);
		}

		// check if what we receive is a Command instead of a data record
		if (input.getValueByField("datapoint").getClass() == Command.class) {
			Command cmd = (Command) input.getValueByField("datapoint");
			// execute the command
			executeCommand(user, cmd, userState);
			if (this.subComponents.contains(cmd.getTarget())) {
				//  propagate the command, 
				// if the targeted component is one of our subcomponents
				this.collector.emit(new Values(user, cmd));
			}
			return;
		}
		// buffer the tuples
		((LinkedList<Tuple>)userState.get(UNAKCED_TUPLES_FIELD_NAME)).add(input);
		
		// process data point
		execute(user, input, userState, input.getSourceGlobalStreamid());
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

	public void emit(StreamRecord<? extends Object> rec) {
		if(!isDryrun && targetStream != null){
			// upload the processed record back to ohmage
			streamStore.upload(targetStream, rec);
		}
		UserState state= userStateMap.get(rec.getUser());
		LinkedList<Tuple> unackedTuple = ((LinkedList<Tuple>)state.get(UNAKCED_TUPLES_FIELD_NAME)); 
		
		// emit a new Tuple, anchoring all the previous tuples
		List<Integer> targetIds = null;
		if(this.subComponents.size() > 0){
			collector.emit(unackedTuple, new Values(rec.getUser(), rec));
		}
		
		while(unackedTuple.size() > 0){
			// ack the latest tuple first! so that if the program crashes inside this loop, the spout still know from when to restart
			collector.ack(unackedTuple.removeLast());
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
		declarer.declare(new Fields("user", "datapoint"));
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
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

	public BaseStatefulBolt() {

	}

}
