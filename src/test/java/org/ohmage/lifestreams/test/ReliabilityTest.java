package org.ohmage.lifestreams.test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Hours;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ohmage.lifestreams.LifestreamsTopologyBuilder;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.spouts.BaseLifestreamsSpout;
import org.ohmage.lifestreams.tasks.SimpleTask;
import org.ohmage.lifestreams.tasks.SimpleTimeWindowTask;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.ohmage.models.OhmageUser;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import backtype.storm.LocalCluster;

@ContextConfiguration({"classpath*:/mainContext.xml", "classpath:/testContext.xml"})
@RunWith(SpringJUnit4ClassRunner.class)
public class ReliabilityTest {

	@Autowired
	LifestreamsTopologyBuilder builder;
	@Autowired
	TickSpout tickSpout;
	@Component
	static class TickSpout extends BaseLifestreamsSpout<Long>{
		public TickSpout() {
			super(1, TimeUnit.SECONDS);
		}

		@Override
		protected Iterator<StreamRecord<Long>> getIteratorFor(final OhmageUser user, final DateTime since) {
			final DateTime nextSec = new DateTime((since.getMillis() + 999)/1000 * 1000);
			return new Iterator<StreamRecord<Long>>(){
				DateTime cur = nextSec;
				@Override
				public boolean hasNext() {
					return true;
				}

				@Override
				public StreamRecord<Long> next() {
					long curTick = cur.getMillis();
					StreamRecord<Long> rec = new StreamRecord<Long>(user, cur, curTick);
					cur = cur.plus(1000);
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return rec;
				}

				@Override
				public void remove() {
					// TODO Auto-generated method stub
					
				}
			};
		}
		
	}
	
	static class OddNumberFilterTask extends SimpleTask<Long>{

		@Override
		public void executeDataPoint(StreamRecord<Long> record) {
			if(record.getData() % 2000 != 0){
				this.createRecord()
					.setTimestamp(record.getTimestamp())
					.setData(record.getData())
					.emit();
					this.checkpoint(record.getTimestamp());

			}

		}
		
	}
	static class CountTask extends SimpleTimeWindowTask<Long>{
		int count = 0;

		@Override
		public void executeDataPoint(StreamRecord<Long> record,
				TimeWindow window) {
			count ++;
			
		}

		@Override
		public void finishWindow(TimeWindow window) {
			Assert.assertEquals((window.getTimeWindowEndTime().getMillis() - window.getTimeWindowBeginTime().getMillis() + 1) / 2000, count);
			LoggerFactory.getLogger(CountTask.class).info("Count: {}. Pass reliability test.", count);
			count = 0;
			this.checkpoint(window.getTimeWindowEndTime());
		}
		
	}
	@Test
	public void run() throws InterruptedException{
		// since when to perform the computation
				DateTime since = new DateTime("2013-1-1");
				/** setup the input and output streams **/

				/** setup the topology **/

				builder.setSpout("TickSpout", tickSpout);
				
				// filter odd number
				builder.setTask("Filter", new OddNumberFilterTask(), "TickSpout");
				
				builder.setTask("Counter", new CountTask(), "Filter").setTimeWindowSize(Hours.ONE);

				builder.setColdStart(false);
				//LocalCluster cluster = builder.submitToLocalCluster("Activity-Count");
				//Thread.sleep(6000);

				
		
	}
}
