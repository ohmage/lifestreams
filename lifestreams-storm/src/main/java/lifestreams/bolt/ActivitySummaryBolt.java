package lifestreams.bolt;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import lifestreams.model.DataPoint;
import lifestreams.model.MobilityDataPoint;
import lifestreams.model.OhmageUser;

import org.joda.time.DateTime;
import org.joda.time.base.BaseSingleFieldPeriod;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.utils.Utils;
import be.ac.ulg.montefiore.run.jahmm.ObservationDiscrete;

public class ActivitySummaryBolt extends BasicLifestreamsBolt{

	public ActivitySummaryBolt(BaseSingleFieldPeriod period) {
		super(period);
	}
	final static Double LONGEST_SAMPLING_PERIOD = 5.5 * 60 * 1000;  // in millisec
	
	// global stream ids of the source bolts
	final static GlobalStreamId MobilityStateStreamId = 
			MobilityEventSmoothingBolt.getDefaultStreamId();
			
	final static GlobalStreamId GeoDiameterStreamId = 
			GeoDistanceBolt.getDefaultStreamId();

	@Override
	protected void executeBatch(OhmageUser user, List<DataPoint> data,
			BasicOutputCollector collector) {
		// accumulate time for each mobility state (in millis)
		EnumMap<MobilityState, Double> accumulatedTime = new EnumMap<MobilityState, Double>(MobilityState.class);
		for(MobilityState state:MobilityState.values()){
			accumulatedTime.put(state, 0.0);
		}
		DateTime prevDt = data.get(0).getTimestamp();
		MobilityState prevState = ((MobilityDataPoint)data.get(0)).getState();
		for(DataPoint dp: data){
			MobilityDataPoint mdp = (MobilityDataPoint)dp;
			MobilityState state = mdp.getState();
			DateTime dt = mdp.getTimestamp();
			long duration = dt.getMillis() - prevDt.getMillis();
			// check if the duration is shorter than the longest sampling period
			// (if not so, we assume there is missing data point)
			if( duration  < LONGEST_SAMPLING_PERIOD ){
				Double halfPeriod = duration / 2.0;
				// either state the sandwiches this duration get a half of the time
				accumulatedTime.put(prevState, accumulatedTime.get(prevState) + halfPeriod);
				accumulatedTime.put(state, accumulatedTime.get(state) + halfPeriod);
			}
			prevState = state;
			prevDt = dt;
		}
		System.out.println(accumulatedTime);
	}

}
