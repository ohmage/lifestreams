package lifestreams.bolt;

import lifestreams.model.StreamRecord;
import lifestreams.utils.KryoSerializer;
import lifestreams.utils.TimeWindow;

import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import state.UserState;
import backtype.storm.topology.BasicOutputCollector;

public class SimpleLifestreamsBolt extends LifestreamsBolt {
	SimpleTask task;

	public SimpleLifestreamsBolt(BaseSingleFieldPeriod timeWindowSize,
			OhmageStream targetStream) {
		super(timeWindowSize, targetStream);
	}

	@Override
	protected void newUser(OhmageUser user, UserState state) {
		super.newUser(user, state);
		SimpleTask newtask = KryoSerializer.getInstance().copy(task);
		newtask.init(user, this);
		state.put("task", newtask);
	}

	@Override
	protected boolean executeDataPoint(OhmageUser user, StreamRecord dp,
			UserState state, TimeWindow window, BasicOutputCollector collector) {
		((SimpleTask) state.get("task")).executeDataPoint(dp, window, collector);
		return false;
	}

	@Override
	protected void finishWindow(OhmageUser user, UserState state,
			TimeWindow window, BasicOutputCollector collector) {

		((SimpleTask) state.get("task")).finishWindow(window, collector);
		Logger logger = LoggerFactory.getLogger(task.getClass());
		logger.info("Finish timeWindow {}", window.getLastInstant());
	}

	@Override
	protected void snapshotWindow(OhmageUser user, UserState state,
			TimeWindow window, BasicOutputCollector collector) {
		((SimpleTask) state.get("task")).snapshotWindow(window, collector);

	}

	public SimpleLifestreamsBolt(SimpleTask task,
			BaseSingleFieldPeriod timeWindowSize) {
		super(timeWindowSize);
		this.task = task;
	}

	public SimpleLifestreamsBolt(SimpleTask task,
			BaseSingleFieldPeriod timeWindowSize, OhmageStream stream) {

		super(timeWindowSize, stream);
		this.task = task;
	}

}
