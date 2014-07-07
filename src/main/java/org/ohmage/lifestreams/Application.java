package org.ohmage.lifestreams;


import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.SimpleCommandLinePropertySource;

class Application {

    public static void main(String[] args) {

        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.getEnvironment().getPropertySources().addFirst(new SimpleCommandLinePropertySource(args));
        if(ctx.getEnvironment().getProperty("ohmage.version").equals("2.0")) {
            ctx.register(Ohmage20AppConfig.class);
        }else{
            ctx.register(Ohmage30AppConfig.class);
        }
        ctx.refresh();
        ctx.getBean(MobilityMovesTopology.class).run();
    }

}
