package org.ohmage.lifestreams;

import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.oauth.MongoTokenRepository;
import org.ohmage.lifestreams.oauth.TokenManager;
import org.ohmage.lifestreams.oauth.TokenRepository;
import org.ohmage.lifestreams.oauth.client.providers.GoogleOAuth;
import org.ohmage.lifestreams.oauth.client.providers.IProvider;
import org.ohmage.lifestreams.oauth.client.providers.OAuth20Provider;
import org.ohmage.lifestreams.oauth.client.providers.OhmageOAuth;
import org.ohmage.lifestreams.spouts.BaseLifestreamsSpout;
import org.ohmage.lifestreams.spouts.Ohmage30StreamSpout;
import org.ohmage.lifestreams.stores.IStreamStore;
import org.ohmage.lifestreams.stores.OhmageStreamStore;
import org.ohmage.models.Ohmage30Server;
import org.ohmage.models.Ohmage30Stream;
import org.ohmage.models.Ohmage30User;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by changun on 6/29/14.
 */


public class Ohmage30AppConfig extends BaseAppConfig{

    @Bean
    public Ohmage30Server ohmageServer(){
        return new Ohmage30Server(env.getProperty("ohmage.server"));
    }
    @Bean
    public Map<String, IProvider> oauthProviderMap(){
        List<IProvider> providers = new ArrayList<IProvider>();
        providers.add(
                new GoogleOAuth(
                        env.getProperty("lifestreams.oauth.google.client.id"),
                        env.getProperty("lifestreams.oauth.google.client.secret")
                )
        );
        providers.add(
                new OhmageOAuth(ohmageServer(),
                        env.getProperty("lifestreams.oauth.ohmage.client.id"),
                        env.getProperty("lifestreams.oauth.ohmage.client.secret"))
        );

        providers.add(
                new OAuth20Provider("moves",
                        "https://api.moves-app.com/oauth/v1/authorize",
                        "moves://app/authorize",
                        "https://api.moves-app.com/oauth/v1/access_token",
                        env.getProperty("com.moves.api.key"),
                        env.getProperty("com.moves.api.secret"))
        );

        Map<String, IProvider> providerMap = new HashMap<String, IProvider>();
        for(IProvider p: providers){
            providerMap.put(p.getName(), p);
        }
        return providerMap;
    }
    @Bean
    public TokenManager tokenManager() {
        TokenRepository<Ohmage30User> repo = new MongoTokenRepository(env.getProperty("mongo.host"));
        // insert test token for test

        return new TokenManager(repo, oauthProviderMap());
    }
    @Bean public OhmageStreamStore ohmageStreamStore(){
        return new OhmageStreamStore(ohmageServer(), tokenManager());
    }
    @Bean
    public Ohmage30Stream mobilityStream(){

        return new Ohmage30Stream("fa3c3b34-996a-41c7-ac4e-38e590499a0f", "2");
    }
    @Bean
    public BaseLifestreamsSpout<MobilityData> mobilitySpout(){
        return new Ohmage30StreamSpout<MobilityData>(ohmageStreamStore(),
                                                    mobilityStream(),
                                                    MobilityData.class,
                                                    since());
    }

    @Override
    public  BaseLifestreamsSpout<MovesSegment> movesSpout() {
        return null;
    }

    @Override
    @Bean
    public IStreamStore streamStore(){
        return ohmageStreamStore();
    }



}
