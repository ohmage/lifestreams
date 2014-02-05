package lifestreams.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.kevinsawicki.http.HttpRequest;

public class OhmageUser implements Serializable{
	OhmageServer _server;
	String _username;
	String _password;

	public OhmageServer getServer(){
		return _server;
	}
	public OhmageUser(String server, String username, String password) {
		super();
		this._server = new OhmageServer(server);
		this._username = username;
		this._password = password;
	}
	public String getUsername(){
		return _username;
	}
	public String getToken() throws OhmageAuthenticationError{
		 		ObjectMapper mapper = new ObjectMapper();
				HashMap<String, String> data = new HashMap<String, String>();
				data.put("user", _username);
				data.put("password", _password);
				data.put("client", OhmageServer.CLIENT_STRING);
				try{
					InputStream res = HttpRequest.post(_server.getAuthenticateURL(), data, false).buffer();
					ObjectNode rootNode = mapper.readValue(res, ObjectNode.class);
					if(rootNode.get("result").asText().equals("success")){	
						return rootNode.get("token").asText();
					}
					else{
						throw new OhmageAuthenticationError(this);
					}
				}
				catch(IOException e){
					throw new OhmageAuthenticationError(this);
				}
			
	}

	@Override
	public String toString(){
		return String.format("User %s on %s", _username, _server);
	}
	
	public class OhmageAuthenticationError extends Exception{
		OhmageUser user;
		OhmageAuthenticationError(OhmageUser u){
			this.user=u;
		}
	}
}
