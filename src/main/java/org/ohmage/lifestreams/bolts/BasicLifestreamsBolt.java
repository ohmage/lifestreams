package org.ohmage.lifestreams.bolts;

import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.state.UserState;
import org.ohmage.lifestreams.tasks.SimpleTask;
import org.ohmage.lifestreams.utils.KryoSerializer;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author changun BasicLifestreamsBolt, based on BaseStatefulBolt and
 *         TimeWindowBolt, makes it easy to develop a stateful bolt compute the
 *         aggregate measurements for fixed-size time windows. To use it, you
 *         have to assign it a SimpleTask object. It will then make a copy of
 *         the task object for each user and invoke the executeDataPoint(), and
 *         finishTimeWindow methods of the object accordingly. It keeps the
 *         state of the every SimpleTask object using the state-keeping
 *         functionality provided by BaseStatefulBolt.
 */
@SuppressWarnings("rawtypes")
public class BasicLifestreamsBolt extends TimeWindowBolt {

	SimpleTask task;


	@Override
	protected void newUser(OhmageUser user, UserState state) {
		super.newUser(user, state);
		// make a copy of the SimpleTask object
		SimpleTask newtask = KryoSerializer.getInstance().copy(task);
		// call the init() method to initialize the object
		newtask.init(user, this);
		// store the object to the UserState
		state.put("task", newtask);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void executeDataPoint(OhmageUser user, StreamRecord dp,
			UserState state, TimeWindow window) {
		((SimpleTask) state.get("task")).executeDataPoint(dp, window);
	}

	@Override
	protected void finishWindow(OhmageUser user, UserState state,
			TimeWindow window) {

		((SimpleTask) state.get("task")).finishWindow(window);
		Logger logger = LoggerFactory.getLogger(task.getClass());
		logger.info("Finish timeWindow {} for {}", window.getLastInstant().toLocalDate().toDate(), user);
	}

	@Override
	protected void snapshotWindow(OhmageUser user, UserState state,
			TimeWindow window) {
		((SimpleTask) state.get("task")).snapshotWindow(window);

	}

	/**
	 * @param task
	 *            the computation task to be performed. The BasicLifestreamsBolt
	 *            will make a copy of it for each user.
	 * @param timeWindowSize
	 *            the length of the time window the computation will be
	 *            performed on (such Days.One or Weeks.One, etc).
	 */
	public BasicLifestreamsBolt(SimpleTask task,
			BaseSingleFieldPeriod timeWindowSize) {

		super(timeWindowSize);
		this.task = task;
	}

}
