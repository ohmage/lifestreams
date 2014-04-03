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

@ComponentScan
@EnableAutoConfiguration
@ImportResource({"classpath:/users.xml", "classpath*:/mainContext.xml"})
public class Application {
	public static void main(String[] args) {
	
		ApplicationContext ctx = SpringApplication.run(Application.class, args);

		OhmageUser requester = (OhmageUser) ctx.getBean("requester");
		List<OhmageUser> requestees = new ArrayList<OhmageUser>();
		
		Set<String> requesteeSet = (Set<String>) ctx.getBean("requesteeSet");
		for(String requesteeUsername: requesteeSet){
			requestees.add(new OhmageUser(requester.getServer(), requesteeUsername, null));
		}
		if(requestees.size() == 0){
			Map<OhmageClass, List<String>> userList = requester.getAccessibleUsers();
			// store all the accessible user
			Set<String> accessibleUsers = new HashSet<String>();
			for(List<String> users : userList.values()){
				 accessibleUsers.addAll(users);
			}
			for(String requesteeUsername: accessibleUsers){
				requestees.add(new OhmageUser(requester.getServer(), requesteeUsername, null));
			}
		}
		MobilityMovesTopology topology = ctx.getBean(MobilityMovesTopology.class);
		topology.run(requestees);
	}
}
