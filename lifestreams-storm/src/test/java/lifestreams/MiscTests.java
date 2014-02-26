package lifestreams;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lifestreams.bolt.MobilityEventSmoothingBolt;
import lifestreams.model.MobilityState;

import org.joda.time.Duration;
import org.joda.time.Seconds;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import redis.clients.jedis.Jedis;
import be.ac.ulg.montefiore.run.jahmm.Hmm;
import be.ac.ulg.montefiore.run.jahmm.ObservationDiscrete;
import be.ac.ulg.montefiore.run.jahmm.learn.BaumWelchLearner;

@RunWith(SpringJUnit4ClassRunner.class)
public class MiscTests {
	@Test
	public void testJoda() {
		assertEquals(1, new Duration(Seconds.seconds(60).toStandardDuration()).getStandardMinutes());
	}
	@Test
	public void testHmm(){
		Hmm <ObservationDiscrete<MobilityState>> hmm = MobilityEventSmoothingBolt.createHmmModel();
		
		BaumWelchLearner bwl = new BaumWelchLearner();
		List<List<ObservationDiscrete <MobilityState> >> sequences = new ArrayList<List<ObservationDiscrete <MobilityState> >> ();
		MobilityState[] data = new MobilityState[]{MobilityState.STILL,
			MobilityState.STILL,
			MobilityState.STILL,
			MobilityState.STILL,
			MobilityState.STILL,
			MobilityState.STILL,
			MobilityState.DRIVE,
			MobilityState.DRIVE,
			MobilityState.DRIVE,
			MobilityState.DRIVE,
			MobilityState.DRIVE,
			MobilityState.STILL,
			MobilityState.STILL,
			MobilityState.STILL,
			MobilityState.DRIVE,
			MobilityState.DRIVE,
			MobilityState.DRIVE,
			MobilityState.STILL,
			MobilityState.STILL,
			MobilityState.STILL,
			MobilityState.STILL,
			MobilityState.DRIVE,
			MobilityState.STILL,
			MobilityState.STILL,
			MobilityState.STILL,
			MobilityState.STILL,
			
			MobilityState.RUN,
			MobilityState.WALK,
			MobilityState.RUN,
			MobilityState.STILL};
		
		ObservationDiscrete <MobilityState> obs[] = new ObservationDiscrete[data.length];
		for(int i=0; i<data.length; i++){
			obs[i] = new ObservationDiscrete <MobilityState> (data[i]);
		}
		
		int[] ret = (hmm.mostLikelyStateSequence(Arrays.asList(obs)));
		for(int state: ret){
			System.out.println(MobilityState.values()[state].toString());
		}
		
	}
	@Test
	public void testJdis(){
		Jedis jedis = new Jedis("localhost");
		jedis.set("foo", "bar");
		String value = jedis.get("foo");
		assertEquals(value, "bar");
		
	}
}
