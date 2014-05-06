package org.ohmage.lifestreams.bolts;

import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.state.UserState;
import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.lifestreams.utils.KryoSerializer;
import org.ohmage.models.OhmageUser;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Tuple;

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
public class LifestreamsBolt extends BaseStatefulBolt {

	Task task;
	@Override
	protected void newUser(OhmageUser user, UserState state) {
		// make a copy of the SimpleTask object
		Task newtask = KryoSerializer.getInstance().copy(task);
		// call the init() method to initialize the object
		newtask.init(user, this);
		// store the object to the UserState
		state.put("_task", newtask);
	}

	/**
	 * @param task
	 *            the computation task to be performed. The BasicLifestreamsBolt
	 *            will make a copy of it for each user.
	 * @param timeWindowSize
	 *            the length of the time window the computation will be
	 *            performed on (such Days.One or Weeks.One, etc).
	 */
	public LifestreamsBolt(Task task) {
		this.task = task;
	}

	@Override
	protected void execute(OhmageUser user, Tuple input, UserState state,
			GlobalStreamId source) {
		StreamRecord rec = (StreamRecord) input.getValueByField("datapoint");
		((Task) state.get("_task")).executeDataPoint(rec, source);
		
	}

	@Override
	protected boolean executeCommand(OhmageUser user, Command command,
			UserState state) {
		// TODO Auto-generated method stub
		return false;
	}

}
