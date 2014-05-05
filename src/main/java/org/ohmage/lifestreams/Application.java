package org.ohmage.lifestreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;

@ImportResource({"classpath:/users.xml", "classpath*:/mainContext.xml"})
@ComponentScan
@EnableAutoConfiguration
public class Application {
	public static void main(String[] args) {
	
		ApplicationContext ctx = SpringApplication.run(Application.class, args);
		MobilityMovesTopology topology = ctx.getBean(MobilityMovesTopology.class);
		topology.run();
	}
}
