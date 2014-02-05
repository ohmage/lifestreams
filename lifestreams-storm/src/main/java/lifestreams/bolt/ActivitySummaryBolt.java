package lifestreams.bolt;

import java.util.List;

import lifestreams.model.DataPoint;
import lifestreams.model.OhmageUser;

import org.joda.time.base.BaseSingleFieldPeriod;

import backtype.storm.topology.BasicOutputCollector;

public class ActivitySummaryBolt extends BasicLifestreamsBolt{

	public ActivitySummaryBolt(BaseSingleFieldPeriod period) {
		super(period);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void executeBatch(OhmageUser user, List<DataPoint> data,
			BasicOutputCollector collector) {
		
		
	}

}
