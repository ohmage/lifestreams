package org.ohmage.lifestreams.utils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;

import backtype.storm.generated.GlobalStreamId;

public class PendingBuffer {
	// we keep a buffer for each data stream

	static public class RecordAndSource {
		public final StreamRecord rec;
		public final GlobalStreamId source;
		public final DateTime time;
		public RecordAndSource(StreamRecord rec, DateTime time, GlobalStreamId source) {
			this.rec = rec;
			this.source = source;
			this.time = time;
		}

		public StreamRecord getRecord() {
			return rec;
		}
		public DateTime getTime(){
			return time;
		}
		public GlobalStreamId getSource() {
			return source;
		}
	}

	private List<RecordAndSource> buffer = new LinkedList<RecordAndSource>();
	private Set<GlobalStreamId> pendingStreams = new HashSet<GlobalStreamId>();

	// put into the buffer ordered by time
	public void put(StreamRecord rec, DateTime time, GlobalStreamId source) {
		pendingStreams.add(source);
		for (int i = 0; i < buffer.size(); i++) {
			if (time.isBefore(buffer.get(i).getTime())) {
				buffer.add(i, new RecordAndSource(rec, time, source));
				return;
			}
		}
		buffer.add(new RecordAndSource(rec, time, source));
	}

	public Set<GlobalStreamId> getPendingStreams() {
		return pendingStreams;
	}

	public List<RecordAndSource> getBuffer() {
		return buffer;
	}

	public void clearBuffer() {
		this.buffer.clear();
		this.pendingStreams.clear();
	}

}
