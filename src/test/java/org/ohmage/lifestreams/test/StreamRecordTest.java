package org.ohmage.lifestreams.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.Test;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.models.IUser;
import org.ohmage.models.Ohmage20User;
import org.ohmage.models.OhmageServer;

import java.io.IOException;

public class StreamRecordTest {
    IUser testUser = new Ohmage20User(new OhmageServer("http://test/"), "", "");

    @Test
    public void testTZSerialize() throws IOException {
        ObjectNode node = (ObjectNode) new ObjectMapper().readTree(
                "{\"metadata\":{\"timestamp\":\"2014-06-28T16:16:32.688-04:00\"," +
                        "\"location\":{\"latitude\":40.7127,\"longitude\":74.0059," +
                        "\"provider\":\"\",\"timestamp\":\"2014-06-28T16:16:32.688-04:00\"," +
                        "\"accuracy\":-1.0}}, \"data\":null}");
        String timestamp = "2014-06-28T16:16:32.688-04:00";
        StreamRecord<MobilityData> rec = new StreamRecord.StreamRecordFactory().createRecord(
                node, testUser, MobilityData.class);
        // deserializer must preserve TZ
        Assert.assertEquals(rec.getTimestamp().toString(), timestamp);
        // serializer must preserve TZ
        Assert.assertEquals(rec.toObserverDataPoint().get("metadata").get("timestamp").textValue(),
                timestamp);

    }
}
