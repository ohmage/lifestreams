package org.ohmage.lifestreams.bolts;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.esotericsoftware.kryo.Kryo;
import org.ohmage.lifestreams.LifestreamsConfig;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.stores.IMapStore;
import org.ohmage.lifestreams.stores.IStreamStore;
import org.ohmage.lifestreams.stores.PersistentMapFactory;
import org.ohmage.lifestreams.tasks.Task;
import org.ohmage.lifestreams.tuples.BaseTuple;
import org.ohmage.lifestreams.tuples.GlobalCheckpointTuple;
import org.ohmage.lifestreams.tuples.RecordTuple;
import org.ohmage.lifestreams.tuples.StreamStatusTuple;
import org.ohmage.models.IStream;
import org.ohmage.models.IUser;
import org.ohmage.models.Ohmage20Stream;

import java.util.*;

/**
 * LifestreamsBolt is a implementation of Storm Bolt. Each Lifestreams Bolt is
 * assigned a Task template that contains the main data processing logic. For
 * each user, the bolt will make a copy of the Task template and runs that on
 * each user's data. LifestreamsBolt support the stateful computation, meaning
 * that it is capable of maintain the computation state of a Task and recover
 * the state after crash/failure occurs To make the interface cleaner, instead
 * of directly interacting with the Task object, Lifestreams bolt creates a
 * {@link UserTaskState} instance for each user's Task instance.
 * {@link UserTaskState} maintains the computation states and cache the outputs.
 * Whenever the task commits a checkpoint, the UserState object will make a
 * snapshot of the computation state and store it in a persistent storage. When
 * crash/failure occurs, the LifestreamsBolt can recover/resume the computation
 * state from the persistent storage. LifestreamsBolt provides several function
 * for {@link UserTaskState} to interact with underlying storm topology. For
 * example, {@link #emitRecord(StreamRecord, List, boolean)} is called to emit a
 * stream record to the topology and optionally upload the record to the stream
 * store (if specified).
 *
 * @author changun
 */
public class LifestreamsBolt extends BaseRichBolt implements IGenerator {
    // UserTaskState for each user
    private HashMap<IUser, UserTaskState> userStateMap = new HashMap<IUser, UserTaskState>();

    // storm data collector (for emitting records)
    OutputCollector collector;

    // the name of the topology, and the name of this bolt
    // these values will be populated during prepare phase (see prepare())
    private String topologyId;
    private String componentId;
    private Set<String> sourceIds;
    // the components in the subtree with this bolt as root.
    // (when a bolt receives a command targeting one of its subcomponent,
    // it should propagate the command)
    private Set<String> subComponents;

    // the input streams of this bolt
    private Set<GlobalStreamId> inputStreams;

    // whether to write back the processed records to the ohmage stream.
    private boolean isDryrun = false;
    // the stream store, where the processed data being output to
    private IStreamStore streamStore;
    // the ohmage stream the processed records will be writeback to.
    private IStream targetStream;
    private PersistentMapFactory mapFactory;
    final private Task templateTask;

    private UserTaskState createUserState(IUser user) {
        UserTaskState userState = UserTaskState.createOrRecoverUserState(this,
                user, templateTask, mapFactory);
        userStateMap.put(user, userState);
        return userState;
    }

    private UserTaskState getUserState(IUser user) {
        return userStateMap.get(user);
    }

    public Map<String, UserTaskState> getPersistentStateMap() {
        return mapFactory.getComponentMap(this.componentId, "userTaskState",
                String.class, UserTaskState.class);

    }

    @Override
    public void execute(Tuple input) {
        BaseTuple baseTuple = BaseTuple.createFromRawTuple(input);
        IUser user = baseTuple.getUser();

        if (baseTuple instanceof StreamStatusTuple) {
            StreamStatusTuple tuple = (StreamStatusTuple) baseTuple;
            switch (tuple.getStatus()) {
                case HEAD:
                    createUserState(user);
                    break;
                case END:
                    UserTaskState userState = getUserState(user);
                    if (userState != null) {
                        // reclaim most of fields in the UserState,
                        // but keep the UserState itself so that it
                        // can still receive the process global check point
                        // global check points
                        userState.streamEnd();
                    }
                    break;
                case MIDDLE:
                    break;
            }
            collector.emit(baseTuple.getTuple(), baseTuple.getValues());
            return;
        }
        UserTaskState userState = getUserState(user);

        if (userState != null) {

            if (baseTuple instanceof GlobalCheckpointTuple) {
                userState.executeGlobalCheckpoint((GlobalCheckpointTuple) baseTuple);
            } else if (baseTuple instanceof RecordTuple && !userState.isEnded()) {
                userState.execute((RecordTuple) baseTuple);
            }
        }
    }

    /**
     * Emit a stream record tuple. If we are not doing dryrun, and the target stream is set,
     * upload the record at the same time
     *
     * @param rec     Record to be emitted
     * @param anchors The input tuple this record depends on
     * @param upload  Whether, in addition to emitting it, to upload the tuple to
     *                the defined stream store.
     */
    public void emitRecord(StreamRecord<?> rec,
                           List<Tuple> anchors, boolean upload) {
        if (upload && !isDryrun && targetStream != null) {
            // upload the processed record back to ohmage
            streamStore.upload(targetStream, rec);
        }
        // emit a new Tuple, anchoring the input tuples
        emit(new RecordTuple(rec), anchors);
    }

    /**
     * Emit a tuple to the next bolts.
     *
     * @param tuple   tuple to emit
     * @param anchors a list tuple the emitted tuple depends on
     */
    public void emit(BaseTuple tuple, List<Tuple> anchors) {
        // emit a new Tuple, anchoring all the previous tuples
        if (this.subComponents.size() > 0) {
            collector.emit(anchors, tuple.getValues());
        }
    }

    // DFS to get all the descendants of this Bold
    private Set<String> getSubGraph(TopologyContext context, String rootId,
                                    Set<String> ret) {
        ret.add(rootId);
        for (Map<String, Grouping> child : context.getTargets(rootId).values()) {
            for (String childComponentId : child.keySet()) {
                getSubGraph(context, childComponentId, ret);
            }
        }
        return ret;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(BaseTuple.getFields());
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
                        TopologyContext context, OutputCollector collector) {
        Kryo kryo = SerializationFactory.getKryo(stormConf);
        // create map factory using map store instance specified in the config
        IMapStore mapStore = (IMapStore) LifestreamsConfig
                .getAndDeserializeObject(stormConf,
                        LifestreamsConfig.MAP_STORE_INSTANCE);
        this.mapFactory = new PersistentMapFactory(
                (String) stormConf.get(Config.TOPOLOGY_NAME), mapStore, kryo);

        this.streamStore = (IStreamStore) LifestreamsConfig
                .getAndDeserializeObject(stormConf,
                        LifestreamsConfig.STREAM_STORE_INSTANCE);
        // initialize the topology-wide arguments
        if (stormConf.containsKey(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING)) {
            isDryrun = (Boolean) stormConf
                    .get(LifestreamsConfig.DRYRUN_WITHOUT_UPLOADING);
        }
        this.collector = collector;
        // populate the names of this bolt and the topology
        this.componentId = context.getThisComponentId();
        this.topologyId = context.getStormId();
        // get the source spouts of this component
        this.sourceIds = new HashSet<String>();
        for (String spout : context.getRawTopology().get_spouts().keySet()) {
            Set<String> nodes = new HashSet<String>();
            if (this.getSubGraph(context, spout, nodes).contains(
                    this.componentId)) {
                sourceIds.add(spout);
            }
        }
        // get the descendant of this bolt
        subComponents = new HashSet<String>();
        getSubGraph(context, this.getComponentId(), subComponents);
        // get the input streams of this bolt
        inputStreams = new HashSet<GlobalStreamId>();
        for (GlobalStreamId streamId : context
                .getSources(this.getComponentId()).keySet()) {
            inputStreams.add(streamId);
        }
    }

    /**
     * if the target stream is set and dryrun = false, the processed records
     * will be write back to the given stream stream.
     */
    public void setTargetStream(Ohmage20Stream targetStream) {
        this.targetStream = targetStream;
    }

    public Set<GlobalStreamId> getInputStreams() {
        return inputStreams;
    }

    public String getComponentId() {
        return componentId;
    }

    @Override
    public String getGeneratorId() {
        return getComponentId();
    }

    @Override
    public Set<String> getSourceIds() {
        return this.sourceIds;
    }

    @Override
    public String getTopologyId() {
        return topologyId;
    }

    public LifestreamsBolt(Task templateTask) {
        this.templateTask = templateTask;
    }

}
