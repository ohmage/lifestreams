package org.ohmage.lifestreams.test.activityCount;

import org.joda.time.Days;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ohmage.lifestreams.LifestreamsTopologyBuilder;
import org.ohmage.lifestreams.spouts.Ohmage20StreamSpout;
import org.ohmage.models.Ohmage20Stream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@ContextConfiguration({"classpath*:/mainContext.xml", "classpath:/testContext.xml"})
@RunWith(SpringJUnit4ClassRunner.class)
public class ActivityInstanceCountTopology {
    @Autowired
    private // output stream
            Ohmage20Stream activityInstanceCountStream;

    @Autowired
    private // spout that emits mobility data
            Ohmage20StreamSpout mobilitySpout;

    @Autowired
    private // activityInstanceCounter
            ActivityInstanceCounter activityInstanceCounter;

    @Autowired
    private
    LifestreamsTopologyBuilder builder;

    @Test
    public void run() throws InterruptedException {
        /** setup the input and output streams **/

        /** setup the topology **/

        // set the number of parallelism for each task to be 5
        int parallelismPerTask = 5;

        builder.setSpout("MobilitySpout", mobilitySpout);

        // create a ActivityInstanceCounter task, with MobilitySpout as input source
        builder.setTask("ActivityInstanceCount", activityInstanceCounter, "MobilitySpout")
                .setParallelismHint(parallelismPerTask) // num of parallelism = num of users
                .setTimeWindowSize(Days.ONE) // aggregate the data by days
                .setTargetStream(activityInstanceCountStream); //output data to ohmage


        builder.submitToLocalCluster("Activity-Count");
        while (true) {
            try {
                Thread.sleep(100000000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }


}
