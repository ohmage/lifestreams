package org.ohmage.lifestreams.models.data;

import org.ohmage.lifestreams.bolts.IGenerator;
import org.ohmage.lifestreams.bolts.TimeWindow;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MovesCredentials{
	@JsonProperty("access_token")
	public String getAccessToken() {
		return accessToken;
	}
	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}
	@JsonProperty("refresh_token")
	public String getRefreshToken() {
		return refreshToken;
	}
	public void setRefreshToken(String refreshToken) {
		this.refreshToken = refreshToken;
	}
	public MovesCredentials(){
		
	}
	String accessToken;
	String refreshToken;
	
}
