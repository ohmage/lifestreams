package lifestreams.model;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lifestreams.model.OhmageUser.OhmageAuthenticationError;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.joda.time.DateTime;

import com.github.kevinsawicki.http.HttpRequest;

public class OhmageStream {
	String _observerId;
	String _streamId;
	String _observerVer;
	String _streamVer;
	OhmageUser _requester;
	List<OhmageUser>_requestees;
	DateTime _since;
	DateTime _pointer;
	Boolean started = false;
	private final ScheduledExecutorService _scheduler = Executors
			.newScheduledThreadPool(20);
	LinkedBlockingQueue<DataPoint> _queue = new LinkedBlockingQueue<DataPoint>();

	public OhmageStream(String _observerId, String _observerVer,
			String _streamId, String _streamVer, OhmageUser _user, List<OhmageUser> _requestees,
			DateTime _since) {
		super();
		this._observerId = _observerId;
		this._streamId = _streamId;
		this._observerVer = _observerVer;
		this._streamVer = _streamVer;
		this._requester = _user;
		this._requestees = _requestees;
		this._since = _since;
		this._pointer = _since;
	}

	public String getObserverId() {
		return _observerId;
	}

	public String getStreamId() {
		return _streamId;
	}

	public OhmageUser getUser() {
		return _requester;
	}

	public DateTime getSince() {
		return _since;
	}

	public void start() {
		// stat a fetcher thread for each requestee
		for(OhmageUser requestee: this._requestees){
			Runnable fetcher = new Fetcher(requestee);
			_scheduler.scheduleWithFixedDelay(fetcher, 0, 5, TimeUnit.SECONDS);
		}
		// schedule the every five second (after the previous one terminates)

		this.started = true;
	}

	public void stop() {
		_scheduler.shutdown();
		this.started = false;
	}

	public DataPoint poll() throws InterruptedException {
		return _queue.take();
	}



	public class Fetcher implements Runnable {
		JsonFactory factory = new MappingJsonFactory();
		OhmageUser _requestee;
		
		Fetcher(OhmageUser requestee){
			this._requestee = requestee;
		}
		
		Map<String, String> getRequestParams(String token) {
			HashMap<String, String> params = new HashMap<String, String>();
			params.put("auth_token", token);
			params.put("observer_id", _observerId);
			params.put("stream_id", _streamId);
			params.put("start_date", _pointer.toString());
			params.put("observer_version", _observerVer);
			params.put("stream_version", _streamVer);
			params.put("client", OhmageServer.CLIENT_STRING);
			params.put("username", _requestee.getUsername());
			return params;
		}
		
		String processInputStreamAndReturnNextURL(InputStream buf) throws IOException, InterruptedException{
		    JsonParser jp = factory.createParser(buf);
		    jp.nextToken(); // START {
		    jp.nextToken(); // FIELD NAME
		    String fieldName = jp.getCurrentName();
			assert fieldName.equals("result");
			jp.nextToken(); // VALUE
		    // move from field name to field value
		    String result = jp.getText();
			if (result.equals("success")) {
				jp.nextToken(); // FIELD NAME
				fieldName = jp.getCurrentName();
				assert fieldName.equals("metadata") ;
				jp.nextToken();
				ObjectNode metadata = jp.readValueAsTree();
				jp.nextToken(); // FIELD NAME
				fieldName = jp.getCurrentName();
				assert fieldName.equals("data");
				// skip Array starting token (i.e. [)
				jp.nextToken(); // ARRAY START
				// go over each entry in the array
				while (jp.nextToken() == JsonToken.START_OBJECT) {
					 JsonNode entry = jp.readValueAsTree();
					 DateTime timestamp = DateTime.parse(entry.get("metadata")
							.get("timestamp").asText());
					 ObjectNode data_metadata = (ObjectNode)entry.get("metadata");
					 ObjectNode data = (ObjectNode)entry.get("data");
					 DataPoint dp = new DataPoint(_requestee, timestamp, data, data_metadata);
					_queue.put(dp);
					_pointer = DateTime.parse(entry.get("metadata")
							.get("timestamp").asText());
				}
				
				/* check if there are more results */
				if (metadata.get("next") != null) {
					return metadata.get("next").asText();
				}
				else{
					return null;
				}
			}
			// something wrong wit the returned results
			throw new IOException("Stream Read failed");
		}
		public void run() {
			try {
				/* post request */
				String token = _requester.getToken();
				Map<String, String> params = getRequestParams(token);
				InputStream buf = HttpRequest.post(
						_requester.getServer().getStreamReadURL(), params, false)
						.buffer();
				/* parse the results in a streaming way */
				while (true) {
					// process the input stream, and get the next url (if any)
					String nextURL = processInputStreamAndReturnNextURL(buf);
					buf.close();
					
					// if nextURL is not null, it means we have more data coming in
					if(nextURL != null){
						buf = HttpRequest.get(nextURL).buffer();
					}
					else
						break;
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				return;
			} catch (OhmageAuthenticationError e) {
				e.printStackTrace();
			}
		}
	}
}
