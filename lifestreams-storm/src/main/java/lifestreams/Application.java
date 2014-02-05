package lifestreams;

import lifestreams.model.OhmageStream;
import lifestreams.spout.OhmageObserverSpout;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

@ComponentScan
@EnableAutoConfiguration
public class Application {
    public static void main(String[] args) {
    
    }
}
