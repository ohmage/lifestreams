package lifestreams.utils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import lifestreams.model.StreamRecord;

import org.joda.time.DateTime;

import backtype.storm.generated.GlobalStreamId;

public class PendingBuffer {
	// we keep a buffer for each data stream

	static public class RecordAndSource {
		public final StreamRecord data;
		public final GlobalStreamId source;

		public RecordAndSource(StreamRecord data, GlobalStreamId source) {
			this.data = data;
			this.source = source;
		}

		public StreamRecord getData() {
			return data;
		}

		public GlobalStreamId getSource() {
			return source;
		}
	}

	private List<RecordAndSource> buffer = new LinkedList<RecordAndSource>();
	private Set<GlobalStreamId> pendingStreams = new HashSet<GlobalStreamId>();

	public void put(StreamRecord dp, GlobalStreamId source) {
		pendingStreams.add(source);
		DateTime timestamp = dp.getTimestamp();
		for (int i = 0; i < buffer.size(); i++) {
			if (timestamp.isBefore(buffer.get(i).data.getTimestamp())) {
				buffer.add(i, new RecordAndSource(dp, source));
				return;
			}
		}
		buffer.add(new RecordAndSource(dp, source));
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
