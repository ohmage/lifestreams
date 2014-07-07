package org.ohmage.lifestreams.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.ohmage.models.IUser;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

@JsonIgnoreProperties(ignoreUnknown = true)

public class StreamRecord<T> {
    static ObjectMapper mapper = new ObjectMapper();

    static {
        // register custom datetime serializer/deserelizer
        // which will use the timezone specified in the DateTime object / or json string as default
        mapper.registerModule(new DateTimeSerializeModule());
        // ignore unknown field when deserializing objects
        mapper.configure(
                com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false);
    }

    static public ObjectMapper getObjectMapper() {
        return mapper;
    }

    private IUser user;
    private StreamMetadata metadata = new StreamMetadata();
    private T data;

    public String toString() {
        return String.format("Time:%s\nLocation:%s\nData:%s\n",
                this.getTimestamp(),
                this.getLocation() == null ? "NA" : this.getLocation().toString(),
                this.getData().toString());

    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    // alias for getData
    public T d() {
        return data;
    }

    // alias for setData
    public void d(T data) {
        this.data = data;
    }

    @JsonIgnore
    public DateTime getTimestamp() {
        return this.metadata.getTimestamp();
    }

    @JsonIgnore
    public GeoLocation getLocation() {
        return this.metadata.getLocation();
    }

    @JsonIgnore
    public IUser getUser() {
        return user;
    }

    @JsonIgnore
    public void setTimestamp(DateTime timestamp) {
        this.metadata.setTimestamp(timestamp);
    }

    @JsonIgnore
    public void setUser(IUser user) {
        this.user = user;
    }

    @JsonIgnore
    public void setLocation(GeoLocation location) {
        this.metadata.setLocation(location);
    }

    public StreamMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(StreamMetadata metadata) {
        this.metadata = metadata;
    }

    public StreamRecord(IUser user, DateTime timestamp) {
        this.user = user;
        this.setTimestamp(timestamp);
    }

    public StreamRecord(IUser user, DateTime timestamp,
                        GeoLocation location) {
        this.user = user;
        this.setTimestamp(timestamp);
        this.setLocation(location);
    }

    public StreamRecord(IUser user, DateTime timestamp, T data) {
        this.user = user;
        this.setTimestamp(timestamp);
        this.setData(data);
    }

    public StreamRecord(IUser user, DateTime timestamp,
                        GeoLocation location, T data) {
        this.user = user;
        this.setTimestamp(timestamp);
        this.setLocation(location);
        this.setData(data);
    }

    public ObjectNode toObserverDataPoint() {
        return mapper.convertValue(this, ObjectNode.class);
    }

    public static class StreamRecordFactory implements Serializable {
        public StreamRecordFactory() {
        }

        public <T> StreamRecord<T> createRecord(Object node, org.ohmage.models.IUser user, Class<T> dataClass)
                throws IOException {
            JavaType type = mapper.getTypeFactory().constructParametricType
                    (StreamRecord.class, dataClass);
            //** a hack that renames "meta_data" to "metadata" for ohmage3.0
            if(node instanceof ObjectNode){
                if(((ObjectNode) node).has("meta_data")){
                    ((ObjectNode) node).put("metadata", ((ObjectNode) node).get("meta_data"));
                }
            }
            StreamRecord<T> dataPoint = mapper.convertValue(node, type);
            dataPoint.setUser(user);
            return dataPoint;
        }
        public <T>Iterator<StreamRecord<T>> createIterator(final Iterator<ObjectNode> iter, final IUser user,
                                                           final Class<T> dataClass){
            return new Iterator<StreamRecord<T>>() {
                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public StreamRecord<T> next() {
                    try {
                        return createRecord(iter.next(), user, dataClass);
                    }catch(IOException e){
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public StreamRecord() {
    }
}
