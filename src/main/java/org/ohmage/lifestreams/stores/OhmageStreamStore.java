package org.ohmage.lifestreams.stores;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.oauth.Scope;
import org.ohmage.lifestreams.oauth.TokenManager;
import org.ohmage.models.*;

import java.io.IOException;
import java.util.Iterator;

public class OhmageStreamStore implements IStreamStore {

    final private TokenManager tokenManager;
    final private Ohmage30Server server;
    final private StreamRecord.StreamRecordFactory pointFactory = new StreamRecord.StreamRecordFactory();
    public OhmageStreamStore(Ohmage30Server server, TokenManager tokenManager) {
        this.server = server;
        this.tokenManager = tokenManager;
    }
    public TokenManager getTokenManager(){
        return  tokenManager;
    }
    @Override
    public void upload(IStream stream, StreamRecord rec) {
        // TODO Auto-generated method stub
    }


    @Override
    public <T> Iterable<StreamRecord<T>> query(IStream stream, final IUser user,
                                           final DateTime start, final DateTime end,
                                           final SortOrder order, final int maxRows, final Class<T> c) {
        if(!(stream instanceof Ohmage30Stream)){
            throw new RuntimeException("Only Ohmage30Stream's are supported");
        }
        if(!(user instanceof Ohmage30User)){
            throw new RuntimeException("Only Ohmage30User's are supported");
        }
        final Ohmage30Stream stream30 = (Ohmage30Stream)stream;
        final Ohmage30User user30 = (Ohmage30User) user;
        final Scope scope = new Scope("ohmage", ((Ohmage30Stream) stream).getReadScopeName());
         return new Iterable<StreamRecord<T>>(){
            @Override
            public Iterator<StreamRecord<T>> iterator() {
                String token = tokenManager.getToken(user30, scope).getAccessToken();
                try {
                    return pointFactory.createIterator(new Ohmage30StreamIterator(token, server, stream30, order, start,
                            end, maxRows), user, c);
                } catch (IOException e) {
                    token  = tokenManager.refreshToken(user30, scope).getAccessToken();
                    try {
                        return pointFactory.createIterator(new Ohmage30StreamIterator(token, server, stream30, order, start,
                                end, maxRows), user, c);
                    }
                    catch (IOException ee){
                        throw new RuntimeException(ee);
                    }
                }

            }
        };

    }


}
