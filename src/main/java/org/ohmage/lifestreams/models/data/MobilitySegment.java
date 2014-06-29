package org.ohmage.lifestreams.models.data;

import com.javadocmd.simplelatlng.LatLng;
import fr.dudie.nominatim.model.Address;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.ohmage.lifestreams.models.StreamRecord;

import java.util.List;

public class MobilitySegment {

    public enum State {
        Place, Moves
    }

    static public abstract interface Segment {
        DateTime getBegin();

        DateTime getEnd();

        State getState();

        Double getTimeSpanInSecs();
    }

    static public class MoveSegment implements Segment {
        final DateTime begin;
        final DateTime end;
        final LatLng from;
        final LatLng dest;
        final List<StreamRecord> trackpoints;

        public MoveSegment(DateTime begin, DateTime end, LatLng from,
                           LatLng dest, List<StreamRecord> trackpoints) {
            super();
            this.begin = begin;
            this.end = end;
            this.from = from;
            this.dest = dest;
            this.trackpoints = trackpoints;
        }

        @Override
        public DateTime getBegin() {
            return begin;
        }

        @Override
        public DateTime getEnd() {
            return end;
        }

        @Override
        public State getState() {
            return State.Moves;
        }

        @Override
        public Double getTimeSpanInSecs() {
            return new Duration(begin, end).getMillis() / 1000.0;
        }
    }

    static public class PlaceSegment implements Segment {

        final Address address;
        final DateTime begin;
        final DateTime end;
        final LatLng coordinates;

        public Address getAddress() {
            return address;
        }

        public LatLng getCoordinates() {
            return coordinates;
        }

        @Override
        public DateTime getBegin() {
            return begin;
        }

        @Override
        public DateTime getEnd() {
            return end;
        }

        @Override
        public State getState() {
            return State.Place;
        }

        @Override
        public Double getTimeSpanInSecs() {
            return new Duration(begin, end).getMillis() / 1000.0;
        }

        public PlaceSegment(DateTime begin, DateTime end, LatLng coordinates, Address address) {
            super();
            this.address = address;
            this.begin = begin;
            this.end = end;
            this.coordinates = coordinates;
        }
    }


    private Segment segment;

    public Double getTimeSpanInSecs() {
        return segment.getTimeSpanInSecs();
    }

    public Segment getSegment() {
        return segment;
    }

    public State getState() {
        return this.segment.getState();
    }

    public MobilitySegment(Segment seg) {
        this.segment = seg;
    }
}
