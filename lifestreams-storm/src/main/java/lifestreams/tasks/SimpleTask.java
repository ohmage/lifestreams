package lifestreams.tasks;

import java.io.Serializable;

import lifestreams.bolts.IGenerator;
import lifestreams.bolts.TimeWindow;
import lifestreams.bolts.TimeWindowBolt;
import lifestreams.models.GeoLocation;
import lifestreams.models.StreamRecord;
import lifestreams.models.data.LifestreamsData;

import org.joda.time.DateTime;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author changun
 * 
 *         SimpleTask is the recommended way to implement a Lifestreams module.
 *         Each simple task instance performs a certain computation for one
 *         single user (i.e. it is guaranteed that the same simple task instance
 *         will only receive, and is the only one to receive, the same user's
 *         data. ). The computation is assumed to be performed on a fixed-sized
 *         time window (e.g. the "daily" geo-diameter, the weekly moving
 *         patterns etc.) The The Lifestreams framework will maintain a time
 *         window for each SimpleTask (see BasicLifestreamsBolt, and
 *         TimeWindowBolt).
 * 
 *         The init() method will be called when the task instance just created.
 *         It is the place where you can initialize the state of the
 *         computation.
 * 
 *         The executeDataPoint() will be called when receiving a record in the
 *         current time window. It is typical to update the computation state
 *         with the new data point in this method.
 * 
 *         The finishWindow() method will be called when we have received all
 *         the records for the current time window. It is typical to finalize
 *         the computation, emit the output record, and re-initialize the
 *         computation states for the next time window in this method.
 * 
 *         In addition, the snapshotWindow() method will be called when we
 *         receive a "snapshot" command, which usually happens when the
 *         front-end applications need the partial computation results ASAP even
 *         before receiving all the data for the current time window from the
 *         user. A typical snapshotWindow() method would perform the same
 *         computation as in the finishWindow() method, but without
 *         reinitializing the computation states. emit() and emitSnapshot()
 *         method can be used to emit the output record to the next nodes in the
 *         topology or writeback to ohmage (if the target stream is specified when
 *         creating the topology).
 * @param <INPUT>
 *            the data type the task expects to receive.
 */
public abstract class SimpleTask<INPUT> implements Serializable, IGenerator {

	private OhmageUser user;
	private transient TimeWindowBolt bolt;
	protected Logger logger;

	public TimeWindowBolt getBolt() {
		return bolt;
	}

	public void init(OhmageUser user, TimeWindowBolt bolt) {
		this.user = user;
		this.bolt = bolt;
		logger = LoggerFactory.getLogger(this.getClass());
	}

	
	public abstract void executeDataPoint(StreamRecord<INPUT> dp,
			TimeWindow window);

	public abstract void finishWindow(TimeWindow window);

	public abstract void snapshotWindow(TimeWindow window);

	public class RecordBuilder{
		public GeoLocation getLocation() {
			return location;
		}
		public RecordBuilder setLocation(GeoLocation location) {
			this.location = location;
			return this;
		}
		public DateTime getTimestamp() {
			return timestamp;
		}
		public RecordBuilder setTimestamp(DateTime timestamp) {
			this.timestamp = timestamp;
			return this;
		}
		public LifestreamsData getData() {
			return data;
		}
		public RecordBuilder setData(LifestreamsData data) {
			this.data = data;
			return this;
		}
		public Boolean getIsSnapshot() {
			return isSnapshot;
		}
		public RecordBuilder setIsSnapshot(Boolean isSnapshot) {
			this.isSnapshot = isSnapshot;
			return this;
		}
		public void emit(){
			if(timestamp == null || data ==null){
				throw new RuntimeException("The required filed: timestamp and data are missing");
			}
			StreamRecord rec = new StreamRecord(user, timestamp, location, data);
			if(this.isSnapshot){
				bolt.emitSnapshot(rec);
			}
			else{
				bolt.emit(rec);
			}
		}
		GeoLocation location;
		DateTime timestamp;

		LifestreamsData data;
		Boolean isSnapshot = false;
		
	}

	@Override
	public String getGeneratorId() {
		return this.getClass().getName();
	}

	@Override
	public String getTopologyId() {
		return bolt.getGeneratorId();
	}
	protected RecordBuilder createRecord(){
		return new RecordBuilder();
	}
	public SimpleTask() {

	}
}
