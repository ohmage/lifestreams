package lifestreams.spout;

import java.util.ArrayList;
import java.util.Map;

import org.joda.time.DateTime;

import lifestreams.bolt.ActivitySummaryBolt;
import lifestreams.bolt.CommandSignal;
import lifestreams.model.DataPoint;
import lifestreams.model.OhmageStream;
import lifestreams.model.OhmageUser;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class OhmageObserverSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	OhmageStream _stream;
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

		OhmageUser user = new OhmageUser("https://test.ohmage.org", "changun", "1qaz@WSX");
		// start a OhmageStream for each data contributor
		// TODO: modify this hacky approach....
		ArrayList<OhmageUser> requestees = new ArrayList<OhmageUser>();
		for(String dataContributor: new String[]{"changun",}){
			requestees.add(new OhmageUser("https://test.ohmage.org",  dataContributor, null));
		}
		_stream = new OhmageStream("edu.ucla.cens.Mobility", "2012061300", "extended", "2012050700", 
				                    user, requestees, new DateTime(2000-1-1));
		_stream.start();
	}
	

	@Override
	public void nextTuple() {
		try {
			DataPoint dp = _stream.poll();
			_collector.emit(new Values(dp, dp.getUser(), dp.getTimestamp()));
			if(Math.random() < 0.1){
				CommandSignal signal = new CommandSignal(
							CommandSignal.Command.SNAPSHOT, 
							ActivitySummaryBolt.class.toString());
				_collector.emit(new Values( signal, dp.getUser(), null));
			}
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("datapoint", "user", "timestamp"));

	}
	public OhmageObserverSpout(){


	}
}
