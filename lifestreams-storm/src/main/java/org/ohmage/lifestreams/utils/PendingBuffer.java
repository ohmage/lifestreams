package org.ohmage.lifestreams.utils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Tuple;

public class PendingBuffer {
	// we keep a buffer for each data stream

	static public class RecordAndSource {
		public final Tuple data;
		public final GlobalStreamId source;
		public final DateTime time;
		public RecordAndSource(Tuple data, DateTime time, GlobalStreamId source) {
			this.data = data;
			this.source = source;
			this.time = time;
		}

		public Tuple getData() {
			return data;
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
	public void put(Tuple input, DateTime time, GlobalStreamId source) {
		pendingStreams.add(source);
		for (int i = 0; i < buffer.size(); i++) {
			if (time.isBefore(buffer.get(i).getTime())) {
				buffer.add(i, new RecordAndSource(input, time, source));
				return;
			}
		}
		buffer.add(new RecordAndSource(input, time, source));
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
