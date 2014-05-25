package org.ohmage.lifestreams.models.data;

import org.ohmage.models.OhmageUser;

import co.nutrino.api.moves.impl.dto.authentication.UserMovesAuthentication;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MovesCredentialsData{
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
	@JsonProperty(required=false)
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	@JsonProperty("user_id")
	public long getMovesId() {
		return movesId;
	}
	public void setMovesId(long movesId) {
		this.movesId = movesId;
	}
	@JsonProperty("expires_in")
	public long getExpiredIn() {
		return expiredIn;
	}
	public void setExpiredIn(long expiredIn) {
		this.expiredIn = expiredIn;
	}

	public MovesCredentialsData(){
		
	}
	public static MovesCredentialsData createMovesCredentialsFor(UserMovesAuthentication auth, OhmageUser user){
		MovesCredentialsData c = new MovesCredentialsData();
		c.setAccessToken(auth.getAccess_token());
		c.setRefreshToken(auth.getRefresh_token());
		c.setUsername(user.getUsername());
		c.setExpiredIn(Long.parseLong(auth.getExpires_in()));
		c.setMovesId(Long.parseLong(auth.getUser_id()));
		return c;
	}
	String accessToken;
	String refreshToken;
	String username;
	long movesId;
	long expiredIn;

	
}
