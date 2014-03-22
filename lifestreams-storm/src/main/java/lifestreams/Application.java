package lifestreams;

import java.util.ArrayList;
import java.util.Set;

import org.ohmage.models.OhmageUser;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;
import org.springframework.test.context.ContextConfiguration;

import redis.clients.jedis.Jedis;

@ComponentScan
@EnableAutoConfiguration
@ImportResource({"classpath:/users.xml", "classpath*:/mainContext.xml"})
public class Application {
	public static void main(String[] args) {
		ApplicationContext ctx = SpringApplication.run(Application.class, args);
		Set<String> requesteeSet = (Set<String>) ctx.getBean("requesteeSet");
		OhmageUser requester = (OhmageUser) ctx.getBean("requester");
		ArrayList<OhmageUser> requestees = new ArrayList<OhmageUser>();
		for(String requesteeUsername: requesteeSet){
			requestees.add(new OhmageUser(requester.getServer(), requesteeUsername, null));
		}
		MobilityMovesTopology topology = ctx.getBean(MobilityMovesTopology.class);
		Jedis jedis = new Jedis("localhost");
		jedis.flushDB();
		topology.run(requestees);
	}
}
