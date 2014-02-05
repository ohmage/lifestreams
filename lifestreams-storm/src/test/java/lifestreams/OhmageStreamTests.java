package lifestreams;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;
import lifestreams.bolt.GeoDistanceBolt;
import lifestreams.bolt.MobilityEventSmoothingBolt;
import lifestreams.bolt.MobilityState;
import lifestreams.model.DataPoint;
import lifestreams.model.OhmageStream;
import lifestreams.model.OhmageUser;
import lifestreams.model.OhmageUser.OhmageAuthenticationError;
import lifestreams.spout.OhmageObserverSpout;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Hours;
import org.joda.time.Period;
import org.joda.time.Seconds;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationContextLoader;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import be.ac.ulg.montefiore.run.jahmm.Hmm;
import be.ac.ulg.montefiore.run.jahmm.ObservationDiscrete;
import be.ac.ulg.montefiore.run.jahmm.learn.BaumWelchLearner;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class, loader=SpringApplicationContextLoader.class)
public class OhmageStreamTests {

	@Autowired 
	ApplicationContext context;
	@Autowired
    OhmageUser testuser;
	@Autowired
    OhmageUser wrong_testuser;
	@Test
	public void autentication() throws OhmageAuthenticationError {
		assertNotNull(testuser.getToken());
		try{
			wrong_testuser.getToken();
			fail("Authentication exception was not thrown");
		}
		catch(OhmageAuthenticationError e){

		}
		
	}
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
	public void testTopology() throws InterruptedException, JsonParseException, IOException {

		 TopologyBuilder builder = new TopologyBuilder();
		 builder.setSpout("spout", new OhmageObserverSpout(), 1);
		 Days dur = Days.ONE;
		 
		 builder.setBolt("dummy", new GeoDistanceBolt(dur), 10).fieldsGrouping("spout", new Fields("user"));
		 builder.setBolt(MobilityEventSmoothingBolt.class.getCanonicalName(), new MobilityEventSmoothingBolt(dur), 10).fieldsGrouping("spout", new Fields("user"));
		 Config conf = new Config();
	     conf.setDebug(false);
	     conf.setMaxTaskParallelism(3);
	     conf.registerSerialization(DataPoint.class);
	     LocalCluster cluster = new LocalCluster();
	     cluster.submitTopology("word-count", conf, builder.createTopology());

	     Thread.sleep(100000000);

	     //cluster.shutdown();
		    
		  

	}

}
