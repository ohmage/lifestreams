package org.ohmage.lifestreams.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import co.nutrino.api.moves.exception.OAuthException;
import co.nutrino.api.moves.impl.client.MovesClient;
import co.nutrino.api.moves.impl.client.MovesUserCredentials;
import co.nutrino.api.moves.impl.client.activity.MovesUserStorylineClient;
import co.nutrino.api.moves.impl.client.activity.MovesUserStorylineResourceClient;
import co.nutrino.api.moves.impl.client.authentication.MovesAuthenticationClient;
import co.nutrino.api.moves.impl.client.user.MovesUserClient;
import co.nutrino.api.moves.impl.dto.authentication.UserMovesAuthentication;
import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
import co.nutrino.api.moves.impl.dto.storyline.MovesStoryline;
import co.nutrino.api.moves.impl.dto.user.MovesUser;
import co.nutrino.api.moves.impl.dto.user.MovesUserProfile;
import co.nutrino.api.moves.impl.request.MovesAuthenticationRequestConstructor;
import co.nutrino.api.moves.impl.request.MovesDatesRequestParametersCreator;
import co.nutrino.api.moves.impl.request.MovesResourceRequestConstructor;
import co.nutrino.api.moves.impl.response.MovesResponseHandler;
import co.nutrino.api.moves.impl.service.MovesApi;
import co.nutrino.api.moves.impl.service.MovesOAuthService;
import co.nutrino.api.moves.impl.service.MovesSecurityManager;
import co.nutrino.api.moves.impl.service.MovesServiceBuilder;
import co.nutrino.api.moves.request.RequestTokenConvertor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.MutableDateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.StreamRecord.StreamRecordFactory;
import org.ohmage.lifestreams.models.data.MovesCredentialsData;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.ohmage.models.OhmageUser.OhmageAuthenticationError;
import org.ohmage.sdk.OhmageStreamClient;
import org.ohmage.sdk.OhmageStreamIterator;
import org.ohmage.sdk.OhmageStreamIterator.SortOrder;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MovesSpout extends BaseLifestreamsSpout<MovesSegment>{
	class MovesInfo{
		public MovesInfo(MovesUser user, MovesUserCredentials credentials) {
			this.user = user;
			this.credentials = credentials;
		}
		final MovesUser user;
		final MovesUserCredentials credentials;
	}
	
	// the following are from Moves APIs
    private MovesUserClient userClient;
	private MovesUserStorylineClient storylineClient;
	private MovesAuthenticationClient authClient;
	
	
	// the ohmage stream that stores the moves credentials
    private OhmageStream movesCredentialsStream;

	// record factory for MovesCredentials record in ohmage
    private StreamRecordFactory dataPointFactory = new StreamRecordFactory();
	
	Iterator<StreamRecord<MovesCredentialsData>> getMovesCredentialDataIterator(final OhmageUser user, DateTime after) throws OhmageAuthenticationError, IOException{
		final OhmageStreamIterator iter = new OhmageStreamClient(getRequester())
												.getOhmageStreamIteratorBuilder(movesCredentialsStream, user)
												.order(SortOrder.ReversedChronological)
												.startDate(after)
												.build();
		return new  Iterator<StreamRecord<MovesCredentialsData>>(){
			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}
			@Override
			public StreamRecord<MovesCredentialsData> next() {
				StreamRecord<MovesCredentialsData> rec;
				try {
					rec = dataPointFactory.createRecord(iter.next(), user, MovesCredentialsData.class);
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
	MovesUserCredentials getStoredMovesCredentialsFor(OhmageUser requestee) throws OhmageAuthenticationError, IOException{
		// There are two places that could store the moves credentials for a particular user.
		// 1. the requestee's own moves credentials stream.
		// 2. the requester's moves credentials stream.
		
		StreamRecord<MovesCredentialsData> latestRec= null;
		// query the latest record from the requestee's own credentails stream
		Iterator<StreamRecord<MovesCredentialsData>> iter = getMovesCredentialDataIterator(requestee, null);
		while(iter.hasNext()){
			StreamRecord<MovesCredentialsData> rec = iter.next();
			MovesCredentialsData credentials = rec.getData();
			if(credentials.getUsername() == null || credentials.getUsername().length() == 0){
				// only get the record that belong to the requestee himself (i.e. the record without username)
				latestRec = rec;
				break;
			}
		}
		// query the requester's moves credentials stream and get the latest one that belongs to the user
		iter = getMovesCredentialDataIterator(getRequester(), null);
		
		while(iter.hasNext()){
			StreamRecord<MovesCredentialsData> rec = iter.next();
			if(latestRec != null && rec.getTimestamp().isBefore(latestRec.getTimestamp())){
				break;
			}
			MovesCredentialsData credentials = rec.getData();
			if(credentials.getUsername() != null && credentials.getUsername().equals(requestee.getUsername())){
				latestRec = rec;
				break;
			}
		}
		if(latestRec != null){
			MovesCredentialsData cData = latestRec.getData();
			// create MovesUserCredentials based on it
			MovesUserCredentials credentials = new MovesUserCredentials(
					cData.getAccessToken() , cData.getRefreshToken());
			return credentials;
		}else{
			return null;
		}
	}
	MovesUserCredentials refreshCredentials(MovesUserCredentials oldCredentials, OhmageUser user) throws OAuthException, OhmageAuthenticationError, IOException{
		logger.trace("Try to Refresh Moves OAuth Token For {}", user);
		UserMovesAuthentication newAuth = authClient.refreshAuthentication(oldCredentials);
		logger.trace("Received new OAuth Token For {}", user);
		MovesCredentialsData data = MovesCredentialsData.createMovesCredentialsFor(newAuth, user);
		StreamRecord<MovesCredentialsData> rec = new StreamRecord<MovesCredentialsData>(user, new DateTime(), null, data);
		new OhmageStreamClient(getRequester()).upload(movesCredentialsStream, rec.toObserverDataPoint());
		logger.trace("Upload new OAuth Token For {} to requester's Moves Credential Stream", user);
		// create MovesUserCredentials based on it
		MovesUserCredentials newCredentials = new MovesUserCredentials(
												data.getAccessToken()
											  , data.getRefreshToken());
		return newCredentials;
	}
	MovesInfo getMovesInfo(MovesUserCredentials credentials, OhmageUser ohmageUser){
		try{
			while(true){
				try {
					// if it is not a valid credential, try to refresh it
					if(!authClient.validateAuthentication(credentials)){
						credentials = refreshCredentials(credentials, ohmageUser);
					}
					MovesUser user =  userClient.getUser(credentials);		
					return new MovesInfo(user, credentials);
				} catch(java.lang.IllegalArgumentException e){
					// Moves returns null since we query too fast, sleep for a while and retry
					Thread.sleep(10000);
				}
			}
		}
		catch (Exception e) {
				logger.error("Credential for user {} not working. Error: {}", ohmageUser, e.toString());
				return null;
		}
	}

	List<StreamRecord<MovesSegment>> getMovesData(MovesInfo moves, OhmageUser ohmageUser, DateTime start) {

		MovesUserProfile movesProfile = moves.user.getProfile();
		// get the current time in the user's timezone
		DateTimeZone zone = DateTimeZone.forTimeZone(movesProfile.getCurrentTimeZone());
		
		// get the user's first day with Moves
		DateTime firstDateWithMoves = new MutableDateTime(movesProfile.getFirstDate()).toDateTime();
		// set start time to be the user's first date with Moves or the checkpoint of this user. Whichever is later.
		DateTime startTime =  firstDateWithMoves.isAfter(start)? firstDateWithMoves: start;
		// get the current date at the user's timezone
		DateTime today = DateTime.now(DateTimeZone.forTimeZone(movesProfile.getCurrentTimeZone()));

		// get all storylines from Moves
		List<MovesStoryline> storylines = new ArrayList<MovesStoryline>();
		

		// get moves data til yesterday
		for(DateTime pointer=new DateTime(startTime); pointer.isBefore(today); pointer = pointer.plusDays(7)){
			// query 7 day's data at once
			DateTime end =  pointer.plusDays(6).isBefore(today) ? pointer.plusDays(6) : today;
			try{
				while(true){
					// try til we successfuly get the data from moves without timeout
					try {

						MovesStoryline[] sevenDaysStorylines;
						sevenDaysStorylines = storylineClient.getUserStorylineForDates(
									moves.credentials, pointer.toDate(), end.toDate() );
						Thread.sleep(1000);
						storylines.addAll(Arrays.asList(sevenDaysStorylines));
						break;
					} catch(java.lang.IllegalArgumentException e){
						// Moves returns null. it maybe because we query too fast, so sleep for a while
						Thread.sleep(10000);	
					}
				}
			}catch (Exception e) {
				// some error happened. skip this user.
				logger.error("Fetch data error for {} from {} to {}", ohmageUser, pointer, end);
				logger.error("Trace:", e);
			}
		}
		DateTime lastSegmentTime = null;
		// create stream records based on the received segments
		List<StreamRecord<MovesSegment>> recs = new ArrayList<StreamRecord<MovesSegment>>(); 
		
		for(MovesStoryline storyline: storylines){
			if(storyline != null && storyline.getSegments()!=null){
				LocalDate date = new LocalDate(storyline.getDate());
				DateTimeZone timezone = storyline.getSegments()[0].getEndTime().getZone();
				// get the first/last millisecond of the date at the current time zone
				DateTime endOfDate = date.plusDays(1).toDateTimeAtStartOfDay(timezone).minusMillis(1);
				DateTime startOfDate = date.toDateTimeAtStartOfDay(timezone);
				
				for(MovesSegment segment: storyline.getSegments()){
					// make sure each segment's time span do not go over the range of current day
					if(segment.getStartTime().isBefore(startOfDate))
						segment.setStartTime(startOfDate);
					if(segment.getEndTime().isAfter(endOfDate))
						segment.setEndTime(endOfDate);
					if(segment.getStartTime().isAfter((segment.getEndTime()))){
						// such segments are possible when the timezone changed at the midnight..
						continue;
					}
					StreamRecord<MovesSegment> rec = new StreamRecord<MovesSegment>();
					rec.setData(segment);
					rec.setTimestamp(segment.getEndTime());
					rec.setUser(ohmageUser);
					lastSegmentTime = rec.getTimestamp();
					recs.add(rec);
				}
			}
		}

		List<StreamRecord<MovesSegment>> validRecs = new ArrayList<StreamRecord<MovesSegment>>(); 
		// add the records to the queue 
		for(StreamRecord<MovesSegment> rec: recs){
			if(rec.getTimestamp().compareTo(start) >= 0){
				// only add those records whose timestamp is after the checkpoint (inclusive)
				validRecs.add(rec);
			}
		}
		logger.trace("Get {} Moves Segments For User {} from {} to {}", recs.size(), ohmageUser, startTime, lastSegmentTime);
		return validRecs;

	}

	private void initMovesAPI(){
		
		MovesClient client = new MovesClient(new RequestTokenConvertor(), new MovesOAuthService(new MovesServiceBuilder(new MovesApi(), movesSecurityManger), new MovesResponseHandler(),
			    new MovesResourceRequestConstructor(), new MovesAuthenticationRequestConstructor()));
		userClient = new MovesUserClient(client);
		storylineClient = new MovesUserStorylineClient(new MovesUserStorylineResourceClient(client,new MovesDatesRequestParametersCreator()));
		authClient = new MovesAuthenticationClient(client, movesSecurityManger);
		
	}
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		initMovesAPI();
	}
	private MovesSecurityManager movesSecurityManger;
	public MovesSpout(DateTime since, OhmageStream movesCredentialsStream, String movesApiKey, String movesApiSecret) {
		super(since, 2, TimeUnit.HOURS);
		this.movesSecurityManger = new MovesSecurityManager(movesApiKey, movesApiSecret, "location activity");
		this.movesCredentialsStream = movesCredentialsStream;
	}


	@Override
	protected Iterator<StreamRecord<MovesSegment>> getIteratorFor(OhmageUser user,
			DateTime since) {
		try {
			MovesUserCredentials credentials = this.getStoredMovesCredentialsFor(user);
			if(credentials != null){
				// try to see if it is a valid credentials
				MovesInfo info = getMovesInfo(credentials, user);
				if( info != null){
					List<StreamRecord<MovesSegment>> recs = getMovesData(info, user, since);
					return recs.iterator();
				}
			}
		} catch (Exception e) {
			logger.error("Fetch MovesSegmentError", e);
		}
		return null;
	}

}
