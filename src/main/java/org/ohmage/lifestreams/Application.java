package org.ohmage.lifestreams;

import org.joda.time.Days;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.spouts.MovesSpout;
import org.ohmage.lifestreams.spouts.OhmageStreamSpout;
import org.ohmage.lifestreams.tasks.GeoDiameterTask;
import org.ohmage.lifestreams.tasks.mobility.HMMMobilityRectifier;
import org.ohmage.lifestreams.tasks.mobility.MobilityActivitySummarizer;
import org.ohmage.lifestreams.tasks.mobility.TimeLeaveReturnHome;
import org.ohmage.lifestreams.tasks.moves.FilterDuplicatedSegment;
import org.ohmage.lifestreams.tasks.moves.MovesActivitySummarizer;
import org.ohmage.lifestreams.tasks.moves.TrackPointExtractor;
import org.ohmage.models.OhmageStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;

import co.nutrino.api.moves.impl.service.MovesSecurityManager;

@ImportResource({"classpath:/users.xml", "classpath*:/mainContext.xml"})
@ComponentScan
@EnableAutoConfiguration
public class Application {
	
	public static void main(String[] args) {
		
		ApplicationContext ctx = SpringApplication.run(Application.class, args);
		ctx.getBean(MobilityMovesTopology.class).run();
	}
}
