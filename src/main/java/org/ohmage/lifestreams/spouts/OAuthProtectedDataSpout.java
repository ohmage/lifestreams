package org.ohmage.lifestreams.spouts;

import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.token.OAuthToken;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.StreamRecord.StreamRecordFactory;
import org.ohmage.lifestreams.models.data.AccessTokenData;
import org.ohmage.lifestreams.oauth.client.providers.IProvider;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.models.OhmageUser.OhmageAuthenticationError;
import org.ohmage.sdk.OhmageStreamClient;
import org.ohmage.sdk.OhmageStreamIterator;
import org.ohmage.sdk.OhmageStreamIterator.SortOrder;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@SuppressWarnings("SameParameterValue")
abstract class OAuthProtectedDataSpout<T> extends BaseLifestreamsSpout<T> {

	final private OhmageStream accessRecordStream;
	final private Pattern providerPattern;
	final private Pattern scopePattern;

	private StreamRecordFactory<AccessTokenData> dataPointFactory = StreamRecordFactory.createStreamRecordFactory(AccessTokenData.class);
	
	boolean match(AccessTokenData token){
		return providerPattern.matcher(token.getProvider()).find() && 
				scopePattern.matcher(token.getScope()).find();
	}
	Iterator<StreamRecord<AccessTokenData>> getAccessTokenRecordIterator(final OhmageUser user) throws OhmageAuthenticationError, IOException{
		final OhmageStreamIterator iter = new OhmageStreamClient(getRequester())
											.getOhmageStreamIteratorBuilder(accessRecordStream, user)
											.order(SortOrder.ReversedChronological)
											.build();
		return new Iterator<StreamRecord<AccessTokenData>>() {
			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}

			@Override
			public StreamRecord<AccessTokenData> next() {
				StreamRecord<AccessTokenData> rec;
				try {
					rec = dataPointFactory.createRecord(iter.next(), user);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				return rec;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	AccessTokenData getLatestAccessTokenRecord(OhmageUser user) throws OhmageAuthenticationError, IOException{
		// 1. the requestee's own moves credentials stream.
		// 2. the requester's moves credentials stream.
		String username = user.getUsername();
		StreamRecord<AccessTokenData> latestTokenRec= null;
		// query the latest record from the requestee's own credentails stream
		Iterator<StreamRecord<AccessTokenData>> iter = getAccessTokenRecordIterator(user);
		while(iter.hasNext()){
			StreamRecord<AccessTokenData> rec = iter.next();
			AccessTokenData tokenData = rec.getData();
			if(match(tokenData) && tokenData.getUsername().equals(username)){
				// only get the record that belong to the requestee himself (i.e. the record without username)
				latestTokenRec = rec;
				break;
			}
		}
		// query the requester's moves credentials stream and get the latest one that belongs to the user
		iter = getAccessTokenRecordIterator(getRequester());
		
		while(iter.hasNext()){
			StreamRecord<AccessTokenData> rec = iter.next();
			if(latestTokenRec != null && rec.getTimestamp().isBefore(latestTokenRec.getTimestamp())){
				// the following token data is not possible to be newer than the current latest token
				break;
			}
			else{
				// this token data is newer than the current latest token
				AccessTokenData tokenData = rec.getData();
				if(match(tokenData) && tokenData.getUsername().equals(username)){
					latestTokenRec = rec;
					break;
				}
			}
		}
		if(latestTokenRec != null){
			// found at least one token
			return latestTokenRec.getData();
			
		}else{
			return null;
		}
	}
	OAuthToken refreshAndUploadToken(OAuthToken token, OhmageUser user, IProvider provider) throws OAuthSystemException, OAuthProblemException, OhmageAuthenticationError, IOException{
		OAuthToken newToken = provider.refreshToken(token);
		Object metaInfo = provider.getMetaInfo(newToken);
		AccessTokenData record = new AccessTokenData(user, provider, newToken, metaInfo);
    	new OhmageStreamClient(this.getRequester()).upload(accessRecordStream, record.toOhmageRecord());
    	return newToken;
	}
	OAuthProtectedDataSpout(OhmageStream accessRecordStream, Pattern providerNamePattern, Pattern scopePattern, DateTime since, int retryDelay, TimeUnit unit) {
		super(since, retryDelay, unit);
		this.accessRecordStream = accessRecordStream;
		this.providerPattern = providerNamePattern;
		this.scopePattern = scopePattern;

		// TODO Auto-generated constructor stub
	}

}
