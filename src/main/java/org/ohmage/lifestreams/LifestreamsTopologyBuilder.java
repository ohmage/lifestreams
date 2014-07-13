package org.ohmage.lifestreams;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.joda.time.base.BaseSingleFieldPeriod;
import org.ohmage.lifestreams.bolts.LifestreamsBolt;
import org.ohmage.lifestreams.stores.*;
import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.lifestreams.tasks.TimeWindowTask;
import org.ohmage.lifestreams.utils.KryoSerializer;
import org.ohmage.models.Ohmage20Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
 */

public class LifestreamsTopologyBuilder {

    /**
     * Lifestreams Configuration **
     */
    private IStreamStore streamStore = new RedisStreamStore();
    private IMapStore mapStore = new RedisMapStore();
    private boolean dryRun = false;
    private boolean coldStart = false;
    private int maxSpoutPending = 100000;
    private int msgTimeout = 10 * 60;


    /**
     * internal fields **
     */
    private Logger logger = LoggerFactory.getLogger(LifestreamsTopologyBuilder.class);
    private TopologyBuilder builder = new TopologyBuilder();
    private List<BoltConfig> boltConfigs = new ArrayList<BoltConfig>();


    public boolean isDryRun() {
        return dryRun;
    }

    public LifestreamsTopologyBuilder setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }

    public boolean isColdStart() {
        return coldStart;
    }

    public LifestreamsTopologyBuilder setColdStart(boolean coldStart) {
        this.coldStart = coldStart;
        return this;
    }

    /**
     * Get the persistent stream storage (e.g. RedisStreamStore) will be used in
     * the topology.
     *
     * @return the streamStore
     */
    IStreamStore getStreamStore() {
        return streamStore;
    }

    /**
     * Set the persistent stream storage, that is where the Task with
     * targetStream will output data to.
     *
     * @param streamStore the streamStore to set
     */
    public LifestreamsTopologyBuilder setStreamStore(IStreamStore streamStore) {
        this.streamStore = streamStore;
        return this;
    }

    /**
     * Get the persistent Map storage will be used in the topology.
     *
     * @return the mapStore
     */
    IMapStore getMapStore() {
        return mapStore;
    }

    /**
     * Set the persistent map storage, that is where Spout and Task store the
     * checkpoints and the computation states.
     *
     * @param mapStore the mapStore to set
     */
    public LifestreamsTopologyBuilder setMapStore(IMapStore mapStore) {
        this.mapStore = mapStore;
        return this;
    }

    /**
     * @return the maxSpoutPending
     */
    public int getMaxSpoutPending() {
        return maxSpoutPending;
    }

    /**
     * @param maxSpoutPending the maxSpoutPending to set
     */
    public LifestreamsTopologyBuilder setMaxSpoutPending(int maxSpoutPending) {
        this.maxSpoutPending = maxSpoutPending;
        return this;
    }

    /**
     * The timeout time for tuples
     *
     * @return the msgTimeout
     */
    public int getMsgTimeout() {
        return msgTimeout;
    }

    /**
     * Set the timeout time for tuple. It should be set to the longest possible
     * time it will take for a tuple and its derivatives to be fully processed.
     *
     * @param msgTimeout the msgTimeout to set
     */
    public LifestreamsTopologyBuilder setMsgTimeout(int msgTimeout) {
        this.msgTimeout = msgTimeout;
        return this;
    }

    public class BoltConfig {
        final String id;
        final String source;
        final Task task;
        int parallelism_hint = 1;
        Ohmage20Stream targetStream = null;

        BoltConfig(String id, Task task, String source) {
            this.id = id;
            this.task = task;
            this.source = source;
        }

        public BoltConfig setTargetStream(Ohmage20Stream stream) {
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
            LifestreamsBolt bolt = new LifestreamsBolt(task);
            bolt.setTargetStream(targetStream);

            builder.setBolt(id, bolt, parallelism_hint).fieldsGrouping(source,
                    new Fields("user"));
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

        if (coldStart) {
            new PersistentMapFactory(topologyName, mapStore, null).clearAll();
        }
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, this.getConfiguration(),
                this.createTopology());

        return cluster;
    }

    /**
     * Create a storm topology based on the defined spouts/tasks.
     *
     * @return a new instance of storm topology
     */
    StormTopology createTopology() {

        // create LifestreamsBolts containing specified Tasks
        for (BoltConfig config : boltConfigs) {
            config.buildBolt();
        }
        return builder.createTopology();
    }

    /**
     * Get configuration defined in application properties, which can be
     * overrided by console arguments
     *
     * @return
     */
    Config getConfiguration() {
        Config conf = new Config();
        conf.setDebug(false);
        // serialize the persistent map store instance to be used in the topology
        LifestreamsConfig.serializeAndPutObject(conf, LifestreamsConfig.MAP_STORE_INSTANCE, this.getMapStore());
        // serialize the persistent stream store instance to be used in the topology
        LifestreamsConfig.serializeAndPutObject(conf, LifestreamsConfig.STREAM_STORE_INSTANCE, this.getStreamStore());
        // if it is a dryrun, no data will be writeback to ohmage
        conf.put(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING, dryRun);
        // how long a tuple can live in topology without being acked. when a
        // tuple timeout, the fail() function will be called
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, msgTimeout);
        // use our serializer
        conf.put(Config.TOPOLOGY_KRYO_FACTORY, KryoSerializer.class.getName());
        // how many pending spouts (i.e. spouts in the topology) can be submit
        // for a spout task
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, maxSpoutPending);
        return conf;
    }
}
