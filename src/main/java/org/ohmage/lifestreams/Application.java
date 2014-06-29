package org.ohmage.lifestreams;


import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.SimpleCommandLinePropertySource;

class Application {

    public static void main(String[] args) {

        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.getEnvironment().getPropertySources().addFirst(new SimpleCommandLinePropertySource(args));
        ctx.register(AppConfig.class);
        ctx.refresh();
        ctx.getBean(MobilityMovesTopology.class).run();
    }

}
