package lifestreams.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.base.BaseSingleFieldPeriod;

import lifestreams.model.DataPoint;
import lifestreams.model.OhmageUser;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public abstract class BasicLifestreamsBolt extends LifestreamsBolt {

	public BasicLifestreamsBolt(BaseSingleFieldPeriod period) {
		super(period, 1);
	}

	protected void executeBatch(OhmageUser user,
			Map<GlobalStreamId, StreamStore> data,
			BasicOutputCollector collector) {
		List<DataPoint> buffer = data.get(data.keySet().iterator().next()).getCurrentBatch();
		executeBatch(user, buffer, collector);
	}
	protected abstract void executeBatch(OhmageUser user, List<DataPoint> data, BasicOutputCollector collector);
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "datapoint"));
	}
}
