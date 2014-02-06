package lifestreams.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import backtype.storm.generated.Grouping;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseComponent;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public abstract class LifestreamsBolt extends BaseBasicBolt {
	// we keep a buffer (i.e. a data point list) for each data stream of each user
	Map<OhmageUser, Map<GlobalStreamId, StreamStore>> data_buffer;
	BaseSingleFieldPeriod period;
	TopologyContext context;
	class StreamStore{
		private List<DataPoint> curBatch = new LinkedList<DataPoint>();
		private List<DataPoint> pendingBatch = new LinkedList<DataPoint>();
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
		
		int numberOfExpectedStreams = this.inputStreams.size();
		if(data_buffer.get(user).keySet().size() < numberOfExpectedStreams){
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
		// get the user
		OhmageUser user = (OhmageUser) input.getValueByField("user");
		
		// check if we receive a command instead of data point
		if( input.getValueByField("datapoint").getClass() == CommandSignal.class){
			CommandSignal signal = (CommandSignal)input.getValueByField("datapoint");
			// in case of SNAPSHOT cmd, and the target is a descendant of this bolt
			if(signal.getCmd()== CommandSignal.Command.SNAPSHOT && 
					this.descendantComponents.contains(signal.getTarget())){
				this.snapshot(user, signal, collector);
			}
			return;
		}
		// get the data point from the tuple
		DataPoint dp = (DataPoint) input.getValueByField("datapoint");
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
	
	// the operation to perform when receive snapshot signal 
	// (normally we execute the current batch and emit the result as in normal situation, 
	//  but will stay in the current period indead of moving on)
	protected void snapshot(OhmageUser user, CommandSignal signal, BasicOutputCollector collector){
		if(!data_buffer.get(user).isEmpty()){
			// process the batch immediately, without performing the post execution operation
			executeBatch(user, data_buffer.get(user), collector);
		}
		// send SNAPSHOT command to the downstreaming tasks
		collector.emit(new Values(user, signal));
	}
	
	Set<String> getDscendants(TopologyContext context, String rootId, Set<String> ret){
		for(Map<String, Grouping> child:context.getTargets(rootId).values()){
			for(String childComponentId:child.keySet()){
				ret.add(childComponentId);
				getDscendants(context, childComponentId, ret);
			}
		}
		return ret;
	}
	public String getComponentId(){
		return this.getClass().getCanonicalName();
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "datapoint"));
	}
	Set<String> descendantComponents;
	Set<GlobalStreamId> inputStreams;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    	// get the descendant of this bolt
    	descendantComponents = new HashSet<String>();
		getDscendants(context, this.getComponentId(), descendantComponents);
		// get the expected input streams of this bolt
		inputStreams = new HashSet<GlobalStreamId>();
		for(GlobalStreamId streamId:context.getSources(this.getComponentId()).keySet()){
			inputStreams.add(streamId);
		}
    	
    }
    
    public static GlobalStreamId getDefaultStreamId(){
    	// TODO: this is a terrible way to do so
    	String className = new Object(){}.getClass().getEnclosingClass().getCanonicalName();
    	return new GlobalStreamId(
    			className, Utils.DEFAULT_STREAM_ID);
    }
    public static String getDefaultComponentId(){
    	// TODO: this is a terrible way to do so
    	String className = new Object(){}.getClass().getEnclosingClass().getCanonicalName();
    	return className;
    }
	public LifestreamsBolt(BaseSingleFieldPeriod period){
		this.period = period;
		this.data_buffer = new HashMap<OhmageUser,Map<GlobalStreamId,StreamStore>>();
	}

}
