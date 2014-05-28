package org.ohmage.lifestreams.test;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.MobilityData;

import com.javadocmd.simplelatlng.LatLng;

public class StreamRecordTest {

	@Test
	public void testSerialize(){
		StreamRecord<MobilityData> rec = new StreamRecord<MobilityData>();
		rec.setLocation(new GeoLocation(new DateTime(), new LatLng(40.7127, 74.0059), -1, ""));
		rec.setTimestamp(new DateTime());
		Assert.assertNotNull(rec.toObserverDataPoint().get("data"));
		
	}
}
