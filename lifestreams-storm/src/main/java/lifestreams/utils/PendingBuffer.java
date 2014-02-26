package lifestreams.utils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import lifestreams.model.IDataPoint;

import org.joda.time.DateTime;

import backtype.storm.generated.GlobalStreamId;

public class PendingBuffer {
	// we keep a buffer for each data stream

	static public class DataPointAndSource { 
		  public final IDataPoint data; 
		  public final GlobalStreamId source; 
		  public DataPointAndSource(IDataPoint data, GlobalStreamId source) { 
		    this.data = data; 
		    this.source = source; 
		  }
		public IDataPoint getData() {
			return data;
		}
		public GlobalStreamId getSource() {
			return source;
		} 
	} 
	
	private List<DataPointAndSource> buffer = new LinkedList<DataPointAndSource>();
	private Set<GlobalStreamId> pendingStreams = new HashSet<GlobalStreamId>();
	
	public void put(IDataPoint dp, GlobalStreamId source){
		pendingStreams.add(source);
		DateTime timestamp = dp.getTimestamp();
		for(int i=0; i<buffer.size(); i++){
			if(timestamp.isBefore(buffer.get(i).data.getTimestamp())){
				buffer.add(i, new DataPointAndSource(dp, source));
				return;
			}
		}
		buffer.add(new DataPointAndSource(dp, source));
	}
	public Set<GlobalStreamId> getPendingStreams(){
		return pendingStreams;
	}
	public List<DataPointAndSource> getBuffer(){
		return buffer;
	}
	public void clearBuffer(){
		this.buffer.clear(); 
		this.pendingStreams.clear();
	}
	
}
