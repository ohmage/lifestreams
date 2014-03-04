package lifestreams.bolt;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import state.RedisBoltState;
import state.UserState;
import lifestreams.model.StreamRecord;
import lifestreams.model.data.LifestreamsData;

import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.models.OhmageUser.OhmageAuthenticationError;
import org.ohmage.sdk.OhmageStreamClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public abstract class BaseLifestreamsBolt extends BaseBasicBolt implements
		IGenerator {

	// keep the state for each user in redis
	protected RedisBoltState state;
	// the components in the sub topology with this Bolt as the root
	Set<String> subComponents;
	Set<GlobalStreamId> inputStreams;

	// called when this bolt receive the data of a new user
	abstract protected void newUser(OhmageUser newUser, UserState state);

	// the operation to perform on each incoming data point.
	abstract protected void execute(OhmageUser user, StreamRecord dp,
			UserState state, BasicOutputCollector collector,
			GlobalStreamId source);

	// the operation to perform on each incoming data point.
	abstract protected void executeCommand(OhmageUser user, Command command,
			UserState state, BasicOutputCollector collector);

	// these values will be populated during prepare
	private String topologyId;
	private String componentId;

	// logger
	Logger logger = LoggerFactory.getLogger(BaseLifestreamsBolt.class);

	// target ohmage stream
	private OhmageStream targetStream;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		// get the user
		OhmageUser user = (OhmageUser) input.getValueByField("user");
		UserState userState;
		// check if this is a new user
		if (!state.containUser(user)) {
			userState = state.newUserState(user);
			// new user method should implement the neccesary initialization for
			// the user state
			newUser(user, userState);
		} else {
			userState = state.get(user);
		}

		// check if what we receive is a command instead of data point
		if (input.getValueByField("datapoint").getClass() == Command.class) {
			Command signal = (Command) input.getValueByField("datapoint");
			if (this.subComponents.contains(signal.getTarget())) {

				collector.emit(new Values(user, signal));
			}
			return;
		}
		// extract data point from the tuple
		StreamRecord dp = (StreamRecord) input.getValueByField("datapoint");
		// process data point
		execute(user, dp, userState, collector, input.getSourceGlobalStreamid());
		// persistent any changes we just made
		state.sync(user);
	}

	protected void upload(StreamRecord<? extends LifestreamsData> dp) {

		// only do upload if targetStream is specified
		if (targetStream != null) {
			try {
				new OhmageStreamClient(dp.getUser()).upload(targetStream,
						dp.toObserverDataPoint());
			} catch (OhmageAuthenticationError e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private List<Integer> emitInternal(
			StreamRecord<? extends LifestreamsData> dp,
			BasicOutputCollector collector) {
		// TODO: how to deal with upload failure
		upload(dp);
		return collector.emit(new Values(dp.getUser(), dp));
	}

	public List<Integer> emit(StreamRecord<? extends LifestreamsData> dp,
			BasicOutputCollector collector) {
		dp.d().setSnapshot(false);
		return emitInternal(dp, collector);
	}

	public List<Integer> emitSnapshot(
			StreamRecord<? extends LifestreamsData> dp,
			BasicOutputCollector collector) {
		dp.d().setSnapshot(true);
		return emitInternal(dp, collector);
	}

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
	public void prepare(Map stormConf, TopologyContext context) {
		this.state = new RedisBoltState(context);

		this.componentId = context.getThisComponentId();
		this.topologyId = context.getStormId();
		// get the descendant of this bolt
		subComponents = new HashSet<String>();
		getSubGraph(context, this.getComponentId(), subComponents);
		// get the expected input streams of this bolt
		inputStreams = new HashSet<GlobalStreamId>();
		for (GlobalStreamId streamId : context
				.getSources(this.getComponentId()).keySet()) {
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

	// create a bolt that is upload-enabled
	public BaseLifestreamsBolt(OhmageStream targetStream) {
		this.targetStream = targetStream;
	}

	public BaseLifestreamsBolt() {

	}

}
