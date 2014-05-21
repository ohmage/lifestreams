package org.ohmage.lifestreams;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.Days;
import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.lifestreams.bolts.LifestreamsBolt;
import org.ohmage.lifestreams.spouts.RedisBookkeeper;
import org.ohmage.lifestreams.stores.RedisStreamStore;
import org.ohmage.lifestreams.stores.StreamStore;
import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.lifestreams.tasks.TimeWindowTask;
import org.ohmage.lifestreams.utils.KryoSerializer;
import org.ohmage.models.OhmageStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Lifestreams topology builder provide helper class for defining a Lifestreams
 * topology using {@link #setSpout(String, IRichSpout)} and
 * {@link #setTask(String, Task, String)} and set configuration based on
 * application properties.
 * 
 * @author changun
 * 
 */
/**
 * @author changun
 *
 */
/**
 * @author changun
 *
 */
@Component
public class LifestreamsTopologyBuilder {
	@Autowired(required = false)
	StreamStore defaultStreamStore;

	// Lifestreams Configuration
	@Value("${global.config.dryrun}")
	boolean dryRun;
	@Value("${lifestreams.cold.start}")
	boolean coldStart;
	@Value("${topology.max.spout.pending}")
	int maxSpoutPeding;
	@Value("${topology.message.timeout.secs}")
	int msgTimeout;

	@Autowired
	RedisBookkeeper bookkeeper;

	public boolean isDryRun() {
		return dryRun;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

	public boolean isColdStart() {
		return coldStart;
	}

	public void setColdStart(boolean coldStart) {
		this.coldStart = coldStart;
	}

	TopologyBuilder builder = new TopologyBuilder();
	List<BoltConfig> boltConfigs = new ArrayList<BoltConfig>();

	public class BoltConfig {
		final String id;
		final String source;
		final Task task;
		int parallelism_hint = 1;
		OhmageStream targetStream = null;

		BoltConfig(String id, Task task, String source) {
			this.id = id;
			this.task = task;
			this.source = source;
		}

		public BoltConfig setTargetStream(OhmageStream stream) {
			this.targetStream = stream;
			return this;
		}

		public BoltConfig setTimeWindowSize(BaseSingleFieldPeriod windowSize) {
			if (task instanceof TimeWindowTask) {
				((TimeWindowTask) task).setTimeWindowSize(windowSize);
			} else {
				throw new RuntimeException(
						"Only TimeWindowTask should be assigned a window size.");
			}
			return this;
		}

		public BoltConfig setParallelismHint(int hint) {
			this.parallelism_hint = hint;
			return this;
		}

		private void buildBolt() {
			LifestreamsBolt bolt = new LifestreamsBolt(task, bookkeeper);
			bolt.setTargetStream(targetStream);
			bolt.setStreamStore(defaultStreamStore);
			BoltDeclarer declarer = builder.setBolt(id, bolt, parallelism_hint)
					.fieldsGrouping(source, new Fields("user"));
		}
	}

	public BoltConfig setTask(String id, Task task, String source) {
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

	/**
	 * Create a local cluster and submit the run the defined topology on it with
	 * name <code>topologyName</code>.
	 * 
	 * @param topologyName
	 * @return A local cluster instance
	 */
	public LocalCluster submitToLocalCluster(String topologyName) {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, this.getConfiguration(),
				this.createTopology());

		return cluster;
	}

	/**
	 * Create a storm topology based on the defined spouts/tasks.
	 * @return a new instance of storm topology 
	 */
	public StormTopology createTopology() {
		for (BoltConfig config : boltConfigs) {
			config.buildBolt();
		}
		if (coldStart) {
			this.bookkeeper.clearAll();
		}
		return builder.createTopology();
	}

	/**
	 * Get configuration defined in application properties, which can be overrided by console arguments 
	 * @return
	 */
	public Config getConfiguration() {
		Config conf = new Config();
		conf.setDebug(false);

		// if it is a dryrun, no data will be writeback to ohmage (or the
		// defined stream output store)
		conf.put(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING, dryRun);
		// how long a tuple can live in topology without being acked. when a
		// tuple timeout, the fail() function will be called
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, msgTimeout);
		// use our serializer
		conf.put(Config.TOPOLOGY_KRYO_FACTORY, KryoSerializer.class.getName());
		// how many pending spouts (i.e. spouts in the topology) can be submit
		// for a spout task
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, maxSpoutPeding);

		return conf;

	}
}
