package org.ohmage.lifestreams.models.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.oltu.oauth2.common.token.BasicOAuthToken;
import org.apache.oltu.oauth2.common.token.OAuthToken;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.oauth.client.providers.IProvider;
import org.ohmage.models.OhmageUser;

import java.util.HashMap;
import java.util.Map;

public class AccessTokenData {

	private String provider;
	private String scope;
	private String username;
	private BasicOAuthToken token;
	private Object metaInfo;

	public Object getMetaInfo() {
		return metaInfo;
	}
	public void setMetaInfo(Object metaInfo) {
		this.metaInfo = metaInfo;
	}
	public BasicOAuthToken getToken() {
		return token;
	}
	public void setToken(BasicOAuthToken token) {
		this.token = token;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}

	public String getProvider() {
		return provider;
	}

	public void setProvider(String provider) {
		this.provider = provider;
	}
	public String getScope() {
		return scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}


	public AccessTokenData(OhmageUser user, IProvider provider, OAuthToken token, Object metaInfo){
		this.username = user.getUsername();
		this.provider = provider.getName();
		this.scope = provider.getScope();
		if(token instanceof BasicOAuthToken){
			this.token = (BasicOAuthToken) token;
		}else{
			this.token = new BasicOAuthToken(token.getAccessToken(), token.getExpiresIn(), token.getRefreshToken(), token.getScope());
		}
		
		this.metaInfo = metaInfo;
	}
	public AccessTokenData(){
		
	}
	
	public ObjectNode toOhmageRecord(){
		ObjectMapper mapper = new ObjectMapper();
    	Map<String, Object> record = new HashMap<String, Object>();
    	Map<String, String> metadata = new HashMap<String, String>();
    	metadata.put("timestamp", new DateTime().toString());
    	record.put("metadata", metadata);
    	record.put("data", this);
    	return mapper.convertValue(record, ObjectNode.class);
	}
	
}
