package org.ohmage.lifestreams.tasks;

import java.io.Serializable;
import java.util.Set;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.bolts.IGenerator;
import org.ohmage.lifestreams.bolts.LifestreamsBolt;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.LifestreamsData;
import org.ohmage.models.OhmageUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.GlobalStreamId;

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
 *         The executeDataPoint() will be called when receiving a new record in the
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
public abstract class Task implements Serializable, IGenerator {

	private OhmageUser user;
	private transient LifestreamsBolt bolt;
	private Logger logger;

	public LifestreamsBolt getBolt() {
		return bolt;
	}
	public void init(OhmageUser user, LifestreamsBolt bolt) {
		this.user = user;
		this.bolt = bolt;
		this.logger = LoggerFactory.getLogger(this.getClass());
	}
	public abstract void executeDataPoint(StreamRecord record, GlobalStreamId source);

	public class RecordBuilder{
		GeoLocation location;
		DateTime timestamp;

		Object data;
		Boolean isSnapshot = false;
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
		public Object getData() {
			return data;
		}
		public RecordBuilder setData(Object data) {
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
			if(this.isSnapshot && data instanceof LifestreamsData){
				((LifestreamsData) data).setSnapshot(true);
			}
			bolt.emit(rec);
			
		}
	}

	@Override
	public String getGeneratorId() {
		return this.getClass().getName();
	}
	@Override
	public String getTopologyId() {
		return bolt.getTopologyId();
	}
	@Override
	public Set<String> getSourceIds() {
		return bolt.getSourceIds();
	}
	protected RecordBuilder createRecord(){
		return new RecordBuilder();
	}
	public Logger getLogger() {
		return logger;
	}
	public Task() {

	}
}
