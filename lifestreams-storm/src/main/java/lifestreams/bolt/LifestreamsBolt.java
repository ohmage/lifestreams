package lifestreams.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.Period;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.joda.time.base.BaseSingleFieldPeriod;

import lifestreams.model.DataPoint;
import lifestreams.model.OhmageUser;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public abstract class LifestreamsBolt extends BaseBasicBolt {
	// we keep a buffer (i.e. a data point list) for each data stream of each user
	Map<OhmageUser, Map<GlobalStreamId, StreamStore>> data_buffer;
	BaseSingleFieldPeriod period;
	int numberOfExpectedStreams;
	class StreamStore{
		List<DataPoint> curBatch = new LinkedList<DataPoint>();
		List<DataPoint> pendingBatch = new LinkedList<DataPoint>();
		void put(DataPoint dp){
			if(curBatch.size() == 0 || sameInterval(curBatch.get(0).getTimestamp(), dp.getTimestamp())){
				curBatch.add(dp);
			}
			else{
				pendingBatch.add(dp);
			}
		}
		boolean currentBatchComplete(){
			return pendingBatch.size() > 0;
		}
		List<DataPoint> getCurrentBatch(){
			return curBatch;
		}
	}
	public boolean sameInterval(DateTime dt1, DateTime dt2){
		// TODO: resolve timezone problem. Different datapoint might have different timezones
		// right now we use the timezone of the first data point in the buffer
		MutableDateTime epoch = new MutableDateTime();
		epoch.setZone(dt1.getZone());
		epoch.setDate(0);
		epoch.setTime(0);
		int periodValue = period.getValue(0);
		if(period.getClass() == Years.class && Math.abs(Years.yearsBetween(epoch, dt1).getYears() - Years.yearsBetween(epoch, dt2).getYears()) < periodValue)
			return true;
		else if(period.getClass() == Months.class && Math.abs(Months.monthsBetween(epoch, dt1).getMonths() - Months.monthsBetween(epoch, dt2).getMonths()) < periodValue)
			return true;
		else if(period.getClass() == Weeks.class && Math.abs(Weeks.weeksBetween(epoch, dt1).getWeeks() - Weeks.weeksBetween(epoch, dt2).getWeeks() ) < periodValue)
			return true;
		else if(period.getClass() == Days.class && Math.abs(Days.daysBetween(epoch, dt1).getDays() - Days.daysBetween(epoch, dt2).getDays()) < periodValue)
			return true;
		else if(period.getClass() == Hours.class && Math.abs(Hours.hoursBetween(epoch, dt1).getHours() - Hours.hoursBetween(epoch, dt2).getHours()) < periodValue)
			return true;
		else if(period.getClass() == Minutes.class && Math.abs(Minutes.minutesBetween(epoch, dt1).getMinutes() - Minutes.minutesBetween(epoch, dt2).getMinutes()) < periodValue)
			return true;
		else if(period.getClass() == Seconds.class && Math.abs(Seconds.secondsBetween(epoch, dt1).getSeconds() - Seconds.secondsBetween(epoch, dt2).getSeconds()) < periodValue)
			return true;
		else
			return false;
	}
	
	boolean checkBatchCompletion(OhmageUser user){
		if(data_buffer.get(user).keySet().size() < this.numberOfExpectedStreams){
			return false;
		}
		for(StreamStore stream : data_buffer.get(user).values()){
			if(!stream.currentBatchComplete())
				return false;
		}
		return true;	
	}
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// get the data point from the tuple
		DataPoint dp = (DataPoint) input.getValueByField("datapoint");
		// get the user
		OhmageUser user = (OhmageUser) input.getValueByField("user");
		// create the buffer for this user
		if(!data_buffer.containsKey(user)){
			data_buffer.put(user, new HashMap<GlobalStreamId,StreamStore>());
		}
		if(!data_buffer.get(user).containsKey(input.getSourceGlobalStreamid())){
			data_buffer.get(user).put(input.getSourceGlobalStreamid(), new StreamStore());
		}
		// get the data buffer for this stream
		StreamStore buffer = data_buffer.get(user).get(input.getSourceGlobalStreamid());
		// put the data to the buffer
		if(executeDataPoint(dp, input))
			buffer.put(dp);
		
		 // check if we have completely receive the current batch 
		 if(checkBatchCompletion(user)){
			 // if so, process the current batch
			 executeBatch(user, data_buffer.get(user), collector);
			 // post-processing for the batch (normally we clear the buffer for that user in this function)
			 postExecuteBatch(user, data_buffer.get(user), collector);
		 }
		
	}
	public List<Integer> emit(DataPoint dp, BasicOutputCollector collector){
		return collector.emit(new Values(dp.getUser(), dp));
	}
	// the operation to perform to each incoming tuple.
	protected boolean executeDataPoint(DataPoint dp, Tuple input){
		return true;
	}
	
	// the operation to perform to the data points in the same batch
	protected abstract void executeBatch(OhmageUser user, Map<GlobalStreamId,StreamStore> data, BasicOutputCollector collector);
	
	// the operation to perform after we process the batch (normally we will clear the buffer for that batch)
	protected void postExecuteBatch(OhmageUser user, Map<GlobalStreamId,StreamStore> data, BasicOutputCollector collector){
		data.clear();
	}
	
	// the operation to perform when receive flush signal (this normally happens when the user want the latest result)
	protected void flush(OhmageUser user, List<DataPoint> data, BasicOutputCollector collector){
		// process the batch immediately, without performing the post execution operation
		executeBatch(user, data_buffer.get(user), collector);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "datapoint"));
	}

	public LifestreamsBolt(BaseSingleFieldPeriod period, int numberOfExpectedStreams){
		this.period = period;
		this.data_buffer = new HashMap<OhmageUser,Map<GlobalStreamId,StreamStore>>();
		this.numberOfExpectedStreams = numberOfExpectedStreams;
	}

}
