package org.ohmage.lifestreams.models.data;

import co.nutrino.api.moves.impl.dto.activity.MovesActivity;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.ohmage.lifestreams.models.MobilityState;

import java.io.IOException;
import java.util.*;

public class ActivityEpisode {

    private Set<MobilityState> types = new HashSet<MobilityState>();
    private DateTime endTime;
    private DateTime startTime;
    private double distanceInMiles;
    private List<TrackPoint> trackPoints = new ArrayList<TrackPoint>();
    // number of step in the activity episode. -1 means not applicable
    private int steps = -1;

    public Object getAdditionalFields() {
        return additionalFields;
    }

    public void setAdditionalFields(Object additionalFields) {
        this.additionalFields = additionalFields;
    }

    private Object additionalFields;


    public static class TrackPoint {
        public TrackPoint(double lat, double lng, DateTime time) {
            super();
            this.lat = lat;
            this.lng = lng;
            this.time = time;
        }

        public double getLat() {
            return lat;
        }

        public void setLat(double lat) {
            this.lat = lat;
        }

        public double getLng() {
            return lng;
        }

        public void setLng(double lng) {
            this.lng = lng;
        }

        public DateTime getTime() {
            return time;
        }

        public void setTime(DateTime time) {
            this.time = time;
        }

        double lat, lng;
        DateTime time;
    }


    public Set<MobilityState> getTypes() {
        return types;
    }

    void setTypes(Set<MobilityState> types) {
        this.types = types;
    }

    public DateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(DateTime endTime) {
        this.endTime = endTime;
    }

    public DateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(DateTime startTime) {
        this.startTime = startTime;
    }

    public double getDurationInSeconds() {
        return new Interval(this.getStartTime(), this.getEndTime())
                .toDurationMillis() / 1000;
    }

    public double getDistanceInMiles() {
        return distanceInMiles;
    }

    public void setDistanceInMiles(double distance) {
        this.distanceInMiles = distance;
    }

    public List<TrackPoint> getTrackPoints() {
        return trackPoints;
    }

    public void setTrackPoints(List<TrackPoint> trackPoints) {
        this.trackPoints = trackPoints;
    }

    public int getSteps() {
        return steps;
    }

    public void setSteps(int steps) {
        this.steps = steps;
    }
    public String toString(){
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JodaModule());
            mapper.configure(
                    com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
                    false);
            mapper.setTimeZone(this.getEndTime().getZone().toTimeZone());
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }


    static public ActivityEpisode forMovesActivity(MovesActivity activity) {
        ActivityEpisode instance = new ActivityEpisode();
        // get distanceInMiles in miles
        instance.setDistanceInMiles(activity.getDistance() * 0.000621371192);
        instance.setStartTime(activity.getStartTime());
        instance.setEndTime(activity.getEndTime());
        instance.setSteps(activity.getSteps());
        // set mobility state
        MobilityState state = MobilityState.fromMovesActivity(activity
                .getGroup());
        instance.setTypes(new HashSet<MobilityState>(Arrays.asList(state)));
        // set trackpoints
        if (activity.getTrackPoints() != null) {
            for (co.nutrino.api.moves.impl.dto.activity.TrackPoint tPoint : activity.getTrackPoints()) {
                instance.getTrackPoints().add(
                        new TrackPoint(tPoint.getLat(), tPoint.getLon(), tPoint.getTime())
                );
            }
        }
        // include the raw MovesActivity data as additional fields
        instance.setAdditionalFields(activity);
        return instance;
    }
}
