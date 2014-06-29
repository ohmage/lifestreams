package org.ohmage.lifestreams.tasks;

import com.bbn.openmap.geo.Geo;
import com.bbn.openmap.proj.Length;
import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;
import org.ohmage.lifestreams.models.StreamRecord;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

class ConvexHull {
    public static List<StreamRecord> getHull(List<StreamRecord> points, double toleranceInMeter) {
        // prepare the geolocation array for computing convex hull
        List<Geo> geoPoints = new LinkedList<Geo>();
        for (StreamRecord point : points) {
            geoPoints.add(new Geo(point.getLocation().getCoordinates().getLatitude(),
                    point.getLocation().getCoordinates().getLongitude()));
        }
        double toleranceInRadians = Length.METER.toRadians(toleranceInMeter);
        // get the combined convex hull
        Geo[] hull = com.bbn.openmap.geo.ConvexHull.hull(
                geoPoints.toArray(new Geo[geoPoints.size()]), toleranceInRadians);

        // record the updated convex hull by storing the vertexes of the hull
        List<StreamRecord> hullRecords = new ArrayList<StreamRecord>();
        for (Geo vertex : hull) {
            // get the geolocation of each vertex
            hullRecords.add(points.get(geoPoints.indexOf(vertex)));
        }
        return hullRecords;
    }

    public static double getDiameter(List<StreamRecord> points, LengthUnit unit) {
        double maxDist = 0;
        for (int i = 0; i < points.size(); i++) {
            for (int j = i + 1; j < points.size(); j++) {
                LatLng l1 = points.get(i).getLocation().getCoordinates();
                LatLng l2 = points.get(j).getLocation().getCoordinates();
                double dist = LatLngTool.distance(l1, l2, unit);
                maxDist = dist > maxDist ? dist : maxDist;
            }
        }
        return maxDist;
    }
}
