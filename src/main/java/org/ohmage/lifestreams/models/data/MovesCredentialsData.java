package org.ohmage.lifestreams.models.data;

import co.nutrino.api.moves.impl.dto.authentication.UserMovesAuthentication;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.ohmage.models.IUser;

public class MovesCredentialsData {
    @JsonProperty("access_token")
    public String getAccessToken() {
        return accessToken;
    }

    void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    @JsonProperty("refresh_token")
    public String getRefreshToken() {
        return refreshToken;
    }

    void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }

    @JsonProperty(required = false)
    public String getUsername() {
        return username;
    }

    void setUsername(String username) {
        this.username = username;
    }

    @JsonProperty("user_id")
    public long getMovesId() {
        return movesId;
    }

    void setMovesId(long movesId) {
        this.movesId = movesId;
    }

    @JsonProperty("expires_in")
    public long getExpiredIn() {
        return expiredIn;
    }

    void setExpiredIn(long expiredIn) {
        this.expiredIn = expiredIn;
    }

    public MovesCredentialsData() {

    }

    public static MovesCredentialsData createMovesCredentialsFor(UserMovesAuthentication auth, IUser user) {
        MovesCredentialsData c = new MovesCredentialsData();
        c.setAccessToken(auth.getAccess_token());
        c.setRefreshToken(auth.getRefresh_token());
        c.setUsername(user.getId());
        c.setExpiredIn(Long.parseLong(auth.getExpires_in()));
        c.setMovesId(Long.parseLong(auth.getUser_id()));
        return c;
    }

    private String accessToken;
    private String refreshToken;
    private String username;
    private long movesId;
    private long expiredIn;


}
