package org.ohmage.lifestreams.tasks.moves;

import co.nutrino.api.moves.impl.dto.activity.MovesActivity;
import co.nutrino.api.moves.impl.dto.activity.TrackPoint;
import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
import com.javadocmd.simplelatlng.LatLng;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.TimeWindowData;
import org.ohmage.lifestreams.tasks.SimpleTask;
import org.springframework.stereotype.Component;

/**
 * @author changun This task extracts tracking points from MovesSegements and
 *         outputs dummy stream records with corresponding timestamp and geo
 *         location information.
 */
@Component
public class TrackPointExtractor extends SimpleTask<MovesSegment> {
    class DummyMovesTrackPointData extends TimeWindowData {
        public DummyMovesTrackPointData(TrackPointExtractor generator) {
            super(null, generator);
        }
    }

    private void emitTrackPoint(TrackPoint point) {
        LatLng coordinates = new LatLng(point.getLat(), point.getLon());
        GeoLocation location = new GeoLocation(point.getTime(), coordinates, -1, "Moves");
        this.createRecord()
                .setData(new DummyMovesTrackPointData(this))
                .setLocation(location)
                .setTimestamp(point.getTime())
                .emit();
    }


    @Override
    public void executeDataPoint(StreamRecord<MovesSegment> record) {
        MovesSegment segment = record.d();
        if (segment.getActivities() != null) {
            // if this segment contains activities
            for (MovesActivity activity : segment.getActivities()) {
                // if this activity contains tracking points
                if (activity != null && activity.getTrackPoints() != null) {
                    for (TrackPoint point : activity.getTrackPoints()) {
                        // pull out these track points
                        if (point.getTime() == null) {
                            // set the timestamp as the timestamp of the activity
                            point.setTime(activity.getEndTime());
                        }
                        // emit this track point
                        emitTrackPoint(point);
                    }
                }
            }
        }
        if (segment.getPlace() != null && segment.getPlace().getLocation() != null) {
            // if this segment contains a place
            segment.getPlace().getLocation().setTime(segment.getEndTime());
            // emit the location of this place as a tracking point
            emitTrackPoint(segment.getPlace().getLocation());
        }
        checkpoint(record.getTimestamp());
    }

}
