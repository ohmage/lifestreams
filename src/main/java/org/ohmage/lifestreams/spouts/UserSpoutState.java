package org.ohmage.lifestreams.spouts;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.tuples.SpoutRecordTuple.RecordTupleMsgId;
import org.ohmage.models.IUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;

class UserSpoutState {
    private final IUser user;
    private final BaseLifestreamsSpout spout;
    private final Logger logger = LoggerFactory.getLogger(UserSpoutState.class);

    private DateTime checkpoint;
    // the following are the fields that need to be reset before starting a new batch
    private boolean failed = false;
    private long ackedSerialId = -1;
    private long lastExpectedSerialId = -1;
    private long curBatch = -1;
    private boolean ended = false;
    private long lastCommittedSerial = -1;
    private PriorityQueue<RecordTupleMsgId> ackedSerialIdHeap = new PriorityQueue<RecordTupleMsgId>();


    public long getAckedSerialId() {
        return ackedSerialId;
    }

    public DateTime getCheckpoint() {
        return checkpoint;
    }

    public void setLastCommittedSerial(long serial) {
        this.lastCommittedSerial = serial;
    }

    public long getLastCommittedSerialId() {
        return lastCommittedSerial;
    }

    public long getLastExpectedSerialId() {
        return lastExpectedSerialId;
    }

    public void setLastExpectedSerialId(long batchId, long lastEmittedSerialId) {
        if (this.curBatch == batchId) {
            this.lastExpectedSerialId = lastEmittedSerialId;
        }
    }

    public boolean isFailed() {
        return failed;
    }

    public void setFailed(long batchId, long failedSerialId) {

        if (this.curBatch == batchId) {
            if (lastExpectedSerialId > failedSerialId - 1) {
                lastExpectedSerialId = failedSerialId - 1;
                log();
            }
            if (!ended) {
                // receive fail msg when we are at the middle of the stream
                // it means something is wrong (e.g. Task crashed)
                this.failed = true;
            }
        }
    }

    public boolean isStreamEnded() {
        return ended;
    }

    public void setStreamEnded(boolean ended) {
        this.ended = ended;
    }

    public void newBatch(long batchId) {
        ackedSerialIdHeap = new PriorityQueue<RecordTupleMsgId>();
        this.ackedSerialId = -1;
        this.curBatch = batchId;
        this.failed = false;
        this.ended = false;
        this.lastExpectedSerialId = -1;
        this.lastCommittedSerial = -1;
        logger.info("{} Resume process for {} from {}", spout.getComponentId(), user.getId(), checkpoint);
    }

    private void log() {
        if (getAckedSerialId() == lastExpectedSerialId) {
            if (this.failed) {
                logger.info("{} Failed {} Checkpoint: {}", spout.getComponentId(), user.getId(), getCheckpoint());
            } else {
                logger.info("{} Finish {} Checkpoint: {}", spout.getComponentId(), user.getId(), getCheckpoint());
            }
        }
    }

    public void ackMsgId(RecordTupleMsgId msg) {
        if (msg.getBatchId() == curBatch) {

            // only advance acked serial id by one at a time
            if (msg.getSerialId() == ackedSerialId + 1) {
                ackedSerialId = msg.getSerialId();
                checkpoint = msg.getTime();
                while (!ackedSerialIdHeap.isEmpty()) {
                    // check the min-heap to see if we have more consective serial ids
                    RecordTupleMsgId head = ackedSerialIdHeap.peek();
                    if (head.getSerialId() == ackedSerialId + 1) {
                        ackedSerialId = head.getSerialId();
                        checkpoint = head.getTime();
                        ackedSerialIdHeap.remove();
                    } else {
                        break;
                    }
                }
                // commit the checkpoint to the persistent storage
                spout.commitCheckpointFor(user, checkpoint);
                log();
            } else {
                // if the acked serial id is not the next one we are waiting for,
                // store it in a min-heap
                ackedSerialIdHeap.add(msg);
            }

        }
    }

    public UserSpoutState(IUser user, BaseLifestreamsSpout spout, DateTime checkpoint) {
        this.user = user;
        this.spout = spout;
        this.checkpoint = checkpoint;
    }
}
