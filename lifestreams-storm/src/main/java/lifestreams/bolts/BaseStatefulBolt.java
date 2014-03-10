package lifestreams.bolts;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lifestreams.LifestreamsConfig;
import lifestreams.models.StreamRecord;
import lifestreams.models.data.LifestreamsData;
import lifestreams.state.RedisBoltState;
import lifestreams.state.UserState;

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
 * It assume that the received tuples is shuffled by User and main the computation 
 * state of each user.
 * 
 * It provides the following functionality:
 * 1. Command Receiving and propagation: perform the specified operation when 
 *    receiving a command, and propagate the command if the command targets one of 
 *    its descendant components.
 *    
 * 2. Stateful Computation: (when enableStateful = true) maintain the state of the 
 *    computation for each user in the local redis server.
 *    
 * 3. Writeback: If the target ohmage stream is specified and dryrun == false, the 
 *    processed records will be upload to the specified ohmage stream.
 */
public abstract class BaseStatefulBolt extends BaseRichBolt implements
		IGenerator {

	// storm data collector (for emitting records)
	private OutputCollector collector;
	// whether to store the computation states in the local redis store
	private boolean enableStateful = false;
	// keep the state for each user in the local redis store when enableStateful = true
	protected RedisBoltState state;
	
	// the components in the subtree with this bolt as root.
	// (when a bolt receives a command targeting one of its subcomponent,
	// it should propagate the command)
	Set<String> subComponents;
	
	// the input streams of this bolt
	Set<GlobalStreamId> inputStreams;

	// called when this bolt receive the data of a new user
	abstract protected void newUser(OhmageUser newUser, UserState state);

	// the operation to perform on each incoming record.
	@SuppressWarnings("rawtypes")
	abstract protected void execute(OhmageUser user, StreamRecord record,
			UserState state,GlobalStreamId source);

	// the operation to perform on each incoming command
	abstract protected void executeCommand(OhmageUser user, Command command, UserState state);

	// the name of the topology, and the name of this bolt
	// these values will be populated during prepare phase (see prepare()) 
	private String topologyId;
	private String componentId;

	// logger
	Logger logger = LoggerFactory.getLogger(BaseStatefulBolt.class);

	// whether to write back the processed records to the ohmage stream.
	private boolean isDryrun = false; 
	// the ohmage stream the processed records will be writeback to.
	private OhmageStream targetStream;

	public OhmageStream getTargetStream() {
		return targetStream;
	}
	
	/**
	 * @param targetStream
	 * if the target stream is set and dryrun = false, the processed 
	 * records will be write back to the given ohmage stream. 
	 */
	public void setTargetStream(OhmageStream targetStream) {
		this.targetStream = targetStream;
	}

	@Override
	public void execute(Tuple input) {
		// get the user
		OhmageUser user = (OhmageUser) input.getValueByField("user");
		UserState userState;
		// check if it is a new user
		if (!state.containUser(user)) {
			userState = state.newUserState(user);
			// new user method should implement the neccesary initialization for
			// the user state
			newUser(user, userState);
		} else {
			// else, get the state for this user
			userState = state.get(user);
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
		// extract the record from the tuple
		@SuppressWarnings("rawtypes")
		StreamRecord rec = (StreamRecord) input.getValueByField("datapoint");
		// process data point
		execute(user, rec, userState, input.getSourceGlobalStreamid());
		// persistent any changes we just made if stateful computation is enabled
		if(this.enableStateful){
			state.sync(user);
		}
	}

	protected void upload(StreamRecord<? extends LifestreamsData> dp) {
		try {
			new OhmageStreamClient(dp.getUser()).upload(targetStream,
					dp.toObserverDataPoint());
		} catch (OhmageAuthenticationError e) {
			throw new  RuntimeException(e);
		} catch (IOException e) {
			throw new  RuntimeException(e);
		}
	}

	public List<Integer> emit(StreamRecord<? extends LifestreamsData> dp) {
		if(!isDryrun && targetStream != null){
			// upload the processed record back to ohmage
			upload(dp);
		}
		// emit the processed record into topology
		return collector.emit(new Values(dp.getUser(), dp));
	}

	// through DFS to get all the sub component
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
		if(stormConf.containsKey(LifestreamsConfig.ENABLE_STATEFUL_FUNCTION)){
			enableStateful = (Boolean) stormConf.get(LifestreamsConfig.ENABLE_STATEFUL_FUNCTION);
		}
		if(stormConf.containsKey(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING)){
			isDryrun = (Boolean) stormConf.get(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING);
		}
		this.state = new RedisBoltState(context, enableStateful);
		
		this.collector = collector;
		// populate the names of this bolt and the topology
		this.componentId = context.getThisComponentId();
		this.topologyId = context.getStormId();
		// get the descendant components of this bolt
		subComponents = new HashSet<String>();
		getSubGraph(context, this.getComponentId(), subComponents);
		// get the input streams of this bolt
		inputStreams = new HashSet<GlobalStreamId>();
		for (GlobalStreamId streamId : context.getSources(this.getComponentId()).keySet()) {
			inputStreams.add(streamId);
		}
	}

	public String getComponentId() {
		return componentId;
	}

	@Override
	public String getGeneratorId() {
		return getComponentId();
	}

	@Override
	public String getTopologyId() {
		return topologyId;
	}

	public BaseStatefulBolt() {

	}

}
