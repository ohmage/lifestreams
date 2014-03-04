package lifestreams.bolt;

import java.io.Serializable;

import lifestreams.model.StreamRecord;
import lifestreams.utils.TimeWindow;

import org.ohmage.models.OhmageUser;

import backtype.storm.topology.BasicOutputCollector;

public abstract class SimpleTask<INPUT> implements Serializable, IGenerator {

	private OhmageUser user;
	private transient LifestreamsBolt bolt;

	public OhmageUser getUser() {
		return user;
	}

	public LifestreamsBolt getBolt() {
		return bolt;
	}

	public void init(OhmageUser user, LifestreamsBolt bolt) {
		this.user = user;
		this.bolt = bolt;
	}

	public abstract void executeDataPoint(StreamRecord<INPUT> dp,
			TimeWindow window, BasicOutputCollector collector);

	public abstract void finishWindow(TimeWindow window,
			BasicOutputCollector collector);

	public abstract void snapshotWindow(TimeWindow window,
			BasicOutputCollector collector);

	protected void emit(StreamRecord rec, BasicOutputCollector collector) {
		bolt.emit(rec, collector);
	}

	protected void emitSnapshot(StreamRecord rec, BasicOutputCollector collector) {
		bolt.emitSnapshot(rec, collector);
	}

	@Override
	public String getGeneratorId() {
		return this.getClass().getName();
	}

	@Override
	public String getTopologyId() {
		return bolt.getGeneratorId();
	}

	public SimpleTask() {

	}
}
