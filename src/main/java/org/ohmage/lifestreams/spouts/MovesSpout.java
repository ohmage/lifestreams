package org.ohmage.lifestreams.spouts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.MutableDateTime;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.MovesCredentials;
import org.ohmage.models.OhmageStream;
import org.ohmage.models.OhmageUser;
import org.springframework.beans.factory.annotation.Autowired;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import co.nutrino.api.moves.impl.client.MovesClient;
import co.nutrino.api.moves.impl.client.MovesUserCredentials;
import co.nutrino.api.moves.impl.client.activity.MovesUserStorylineClient;
import co.nutrino.api.moves.impl.client.activity.MovesUserStorylineResourceClient;
import co.nutrino.api.moves.impl.client.user.MovesUserClient;
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

public class MovesSpout extends OhmageStreamSpout{
	MovesUserClient userClient;
	MovesUserStorylineClient storylineClient;
	@Autowired
	MovesSecurityManager movesSecurityManger;
	Map<OhmageUser, MovesUserCredentials> userMapping = new HashMap<OhmageUser, MovesUserCredentials>();
	DateTime lastSyncTime;
	class MovesQueryTooFastException extends Exception{};
	public boolean fetchMovesData(OhmageUser ohmageUser, MovesUserCredentials credentials) throws MovesQueryTooFastException {
		MovesUser user = null;
		try {
			// try to see if this is a valid MovesUserCredentials
			user = userClient.getUser(credentials);
		} catch(java.lang.IllegalArgumentException e){
			// Moves returns null. it maybe because we query too fast, so sleep for a while
			throw new  MovesQueryTooFastException();
		} catch (Exception e) {
			logger.error("Credential for user {} not working. Error: {}", ohmageUser, e.toString());
			return false;
		}
		// we got a working credential, get all the segments for this user.
		MovesUserProfile movesProfile = user.getProfile();
		// get the current time in the user's timezone
		DateTimeZone zone = DateTimeZone.forTimeZone(movesProfile.getCurrentTimeZone());
		
		// get the user's first day with Moves
		MutableDateTime firstDateWithMoves = new MutableDateTime(movesProfile.getFirstDate());

		
		// get data til yesterday
		DateTime yesterday = DateTime.now(DateTimeZone.forTimeZone(movesProfile.getCurrentTimeZone())).minusDays(1);
		
		
		// get all storylines from Moves
		List<MovesStoryline> storylines = new ArrayList<MovesStoryline>();
		
		// get the start time

		DateTime start = firstDateWithMoves.toDateTime();
		

		for(; start.isBefore(yesterday); start = start.plusDays(7)){

				try {
					DateTime end =  start.plusDays(6).isAfter(yesterday) ? yesterday : start.plusDays(6);
					MovesStoryline[] sevenDaysStorylines;
					sevenDaysStorylines = storylineClient.getUserStorylineForDates(
								credentials, start.toDate(), end.toDate() );
					Thread.sleep(1000);
					storylines.addAll(Arrays.asList(sevenDaysStorylines));
				} catch(java.lang.IllegalArgumentException e){
					// Moves returns null. it maybe because we query too fast, so sleep for a while
					throw new  MovesQueryTooFastException();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
		}
		// set start = now, so that, next time, we will query the current day's data again
		// emit all the segments
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
					this.emitRecord(rec);
				}

				
			}
		}
		return true;
	}
		
	public MovesSpout(OhmageStream stream,  DateTime startDate) {
		super(stream, startDate, MovesCredentials.class, false, null);
	}

	@Override
	public void nextTuple() {
		try{
			// sync with Moves if we have not sync for more than 12 hours
			if(lastSyncTime == null || lastSyncTime.plusHours(12).isBefore(DateTime.now())){
				// fetch the moves data for each user with a Moves credentials
				for(OhmageUser user: userMapping.keySet()){
					while(true){
						try {
							fetchMovesData(user, userMapping.get(user));
						} catch (MovesQueryTooFastException e) {
								logger.info("MovesSpout going too fast.. slow dow...");
								Thread.sleep(10000);
								continue;
						}
						break;
					}
					Thread.sleep(1000);
				}
				lastSyncTime = DateTime.now();
			}
			// if ohmage returns some new Moves oauth credentials
			while(!this.getReturnedStreamRecordQueue().isEmpty()){
				try {
					// get a new Moves credential object
					StreamRecord<MovesCredentials> rec = this.getReturnedStreamRecordQueue().take();
					// create MovesUserCredentials based on it
					MovesUserCredentials credentials = new MovesUserCredentials(rec.d().getAccessToken(), rec.d().getRefreshToken());
					// fetch the moves data
					boolean success = false;
					while(true){
						try {
							success = fetchMovesData(rec.getUser(), credentials);
						} catch (MovesQueryTooFastException e) {
							logger.info("MovesSpout going too fast.. slow dow...");
							Thread.sleep(10000);
							continue;
						}
						break;
					}
					if(success){
						// add this to user mapping, so that it will be synced in the future
						userMapping.put(rec.getUser(), new MovesUserCredentials(rec.d().getAccessToken(), rec.d().getRefreshToken()));
					}
				} catch (InterruptedException e) {
					return;
				}
			}
			Thread.sleep(5000);
		}catch(InterruptedException e){
			return;
		}
	}
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		MovesClient client = new MovesClient(new RequestTokenConvertor(), new MovesOAuthService(new MovesServiceBuilder(new MovesApi(), movesSecurityManger), new MovesResponseHandler(),
			    new MovesResourceRequestConstructor(), new MovesAuthenticationRequestConstructor()));
		userClient = new MovesUserClient(client);
		storylineClient = new MovesUserStorylineClient(new MovesUserStorylineResourceClient(client,new MovesDatesRequestParametersCreator()));

	}

}
