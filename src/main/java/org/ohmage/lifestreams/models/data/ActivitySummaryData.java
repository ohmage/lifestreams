package org.ohmage.lifestreams.models.data;

import org.ohmage.lifestreams.bolts.IGenerator;
import org.ohmage.lifestreams.tasks.TimeWindow;

import java.util.ArrayList;
import java.util.List;

public class ActivitySummaryData extends TimeWindowData {

    public double getTotalTimeInSeconds() {
        return totalTime;
    }

    public ActivitySummaryData setTotalTimeInSeconds(double totalTime) {
        this.totalTime = totalTime;
        return this;
    }

    public double getTotalActiveTimeInSeconds() {
        return totalActiveTime;
    }

    public ActivitySummaryData setTotalActiveTimeInSeconds(double totalActiveTime) {
        this.totalActiveTime = totalActiveTime;
        return this;
    }

    public double getTotalActiveDistanceInMiles() {
        double totalMiles = 0.0;
        for (ActivityEpisode epi : this.getActivityEpisodes()) {
            totalMiles += epi.getDistanceInMiles();
        }
        return totalMiles;
    }

    public int getTotalSteps() {
        int totalSteps = -1;
        for (ActivityEpisode epi : this.getActivityEpisodes()) {
            if (epi.getSteps() != -1) {
                totalSteps = totalSteps == -1 ? epi.getSteps() : totalSteps + epi.getSteps();
            }
        }
        return totalSteps;
    }

    public double getTotalSedentaryTimeInSeconds() {
        return totalSedentaryTime;
    }

    public ActivitySummaryData setTotalSedentaryTimeInSeconds(double totalSedentaryTime) {
        this.totalSedentaryTime = totalSedentaryTime;
        return this;
    }

    public double getTotalTransportationTimeInSeconds() {
        return totalTransportationTime;
    }

    public ActivitySummaryData setTotalTransportationTimeInSeconds(
            double totalTransportationTime) {
        this.totalTransportationTime = totalTransportationTime;
        return this;
    }

    public List<ActivityEpisode> getActivityEpisodes() {
        return activityEpisodes;
    }

    public ActivitySummaryData setActivityEpisodes(
            List<ActivityEpisode> activeInstances) {
        this.activityEpisodes = activeInstances;
        return this;
    }

    public double getMaxActiveSpeedInMPH() {
        double maxSpeed = -1;
        for (ActivityEpisode epi : getActivityEpisodes()) {
            // only include active episodes more than 5 mins
            if (epi.getDurationInSeconds() > 5 * 60) {
                maxSpeed = Math.max(epi.getDistanceInMiles() / (epi.getDurationInSeconds() / 3600.0), maxSpeed);
            }
        }
        return maxSpeed;
    }

    private double totalTime;
    private double totalActiveTime;
    private double totalSedentaryTime;
    private double totalTransportationTime;

    private List<ActivityEpisode> activityEpisodes = new ArrayList<ActivityEpisode>();

    public ActivitySummaryData(TimeWindow window, IGenerator generator) {
        super(window, generator);
    }

}
