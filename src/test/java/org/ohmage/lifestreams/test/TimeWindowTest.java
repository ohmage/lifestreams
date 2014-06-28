package org.ohmage.lifestreams.test;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.junit.Assert;
import org.junit.Test;
import org.ohmage.lifestreams.tasks.GeoDiameterTask;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.ohmage.lifestreams.utils.KryoSerializer;


public class TimeWindowTest {
	
	@Test
	public void testWindowEndTime(){
		TimeWindow window = new TimeWindow(Days.ONE, new DateTime("2012-1-1T00:00:00"));
        // end of the timewindow should be the last instant of that window
		Assert.assertEquals(new DateTime("2012-1-2T00:00:00").minus(1), window.getTimeWindowEndTime());
        // check withinWindow method
		Assert.assertFalse(window.withinWindow(new DateTime("2012-1-2T00:00:00")));
		Assert.assertTrue(window.withinWindow(new DateTime("2012-1-2T00:00:00").minus(1)));
		
	}

}


