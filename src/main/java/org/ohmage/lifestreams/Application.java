package org.ohmage.lifestreams;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ohmage.lifestreams.utils.RedisStreamStore;
import org.ohmage.models.OhmageClass;
import org.ohmage.models.OhmageUser;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;

import redis.clients.jedis.Jedis;

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
