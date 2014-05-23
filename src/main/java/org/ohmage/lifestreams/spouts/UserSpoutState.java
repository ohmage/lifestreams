package org.ohmage.lifestreams.spouts;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.ohmage.lifestreams.tuples.SpoutRecordTuple.RecordTupleMsgId;
import org.joda.time.DateTime;
import org.ohmage.models.OhmageUser;

public class UserSpoutState {
	private DateTime checkpoint; 
	private boolean failed = false;
	private long ackedSerialId = -1;
	private long curBatch = -1;
	private boolean ended = false;
	private PriorityQueue<RecordTupleMsgId> ackedSerialIdHeap = new PriorityQueue<RecordTupleMsgId>();
	
	public long getAckedSerialId() {
		return ackedSerialId;
	}
	public DateTime getCheckpoint(){
		return checkpoint;
	}
	public void setFailedWithBatchId(long batchId){
		if(this.curBatch == batchId){
			failed = true;
		}
	}
	public boolean isStreamEnded(){
		return ended;
	}
	public void setStreamEnded(boolean ended){
		this.ended = ended;
	}
	public boolean isFailed(){
		return failed;
	}
	public void newBatch(long batchId){
		ackedSerialIdHeap = new PriorityQueue<RecordTupleMsgId>();
		this.ackedSerialId = -1;
		this.curBatch = batchId;
		this.failed = false;
		this.ended = false;
	}
	public DateTime ackMsgId(RecordTupleMsgId msg){
		if(msg.getBatchId() == curBatch){
			if(msg.getSerialId() == ackedSerialId + 1){ 
				ackedSerialId = msg.getSerialId();
				checkpoint = msg.getTime();
				while(!ackedSerialIdHeap.isEmpty()){
					RecordTupleMsgId head = ackedSerialIdHeap.peek();
					if(head.getSerialId() == ackedSerialId + 1){
						ackedSerialId = head.getSerialId();
						checkpoint = head.getTime();
						ackedSerialIdHeap.remove();
					}else{
						break;
					}
				}
				return checkpoint;
			}else{
				ackedSerialIdHeap.add(msg);
			}
			
		}
		return null;
	}
	long lastCommittedSerial = -1;
	public void setLastCommittedSerial(long serial){
		this.lastCommittedSerial = serial;
	}
	public long getLastCommittedSerialId(){
		return lastCommittedSerial;
	}
	public UserSpoutState(DateTime checkpoint){
		this.checkpoint = checkpoint;
	}
}
