package org.ohmage.lifestreams.utils;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.Days;
import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.lifestreams.bolts.LifestreamsBolt;
import org.ohmage.lifestreams.stores.RedisStreamStore;
import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.lifestreams.tasks.TimeWindowTask;
import org.ohmage.models.OhmageStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
@Component
public class SimpleTopologyBuilder {
	@Autowired RedisStreamStore redisStore;
	TopologyBuilder builder = new TopologyBuilder();
	List<BoltConfig> boltConfigs = new ArrayList<BoltConfig>();
	
	public class BoltConfig{
		final String id;
		final String source;
		final Task task;
		int  parallelism_hint = 1;
		OhmageStream targetStream = null;
		
		BoltConfig(String id, Task task, String source){
			this.id = id;
			this.task = task;
			this.source = source;
		}
		public BoltConfig setTargetStream(OhmageStream stream){
			this.targetStream = stream;
			return this;
		}
		public BoltConfig setTimeWindowSize(BaseSingleFieldPeriod windowSize){
			if(task instanceof TimeWindowTask ){
				((TimeWindowTask)task).setTimeWindowSize(windowSize);
			}else{
				throw new RuntimeException("Only TimeWindowTask should be assigned a window size.");
			}
			return this;
		}
		public BoltConfig setParallelismHint(int hint){
			this.parallelism_hint = hint;
			return this;
		}
		private void buildBolt(){
			LifestreamsBolt bolt = new LifestreamsBolt(task);
			bolt.setTargetStream(targetStream);
			BoltDeclarer declarer = builder.setBolt(id, bolt, parallelism_hint)
					.fieldsGrouping(source, new Fields("user"));
		}
	}

	
	public BoltConfig setTask(String id, Task task, String source){
		BoltConfig config = new BoltConfig(id, task, source);
		this.boltConfigs.add(config);
		// return a BoltConfig object for further configuration
		return config;
	}

	public void setSpout(String id, IRichSpout spout, int parallelism_hint) {
		// add the spout into the topology
		SpoutDeclarer declarer = builder.setSpout(id, spout, parallelism_hint);
		
	}
	public void setSpout(String id, IRichSpout spout) {
		// add the spout into the topology
		SpoutDeclarer declarer = builder.setSpout(id, spout, 1);
		
	}
	public StormTopology createTopology(){
		for(BoltConfig config: boltConfigs){
			config.buildBolt();
		}
		return builder.createTopology();
	}
}
