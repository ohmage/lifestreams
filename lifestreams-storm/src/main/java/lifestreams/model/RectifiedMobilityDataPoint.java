package lifestreams.model;

import lifestreams.bolt.BaseLifestreamsBolt;
import lifestreams.utils.TimeWindow;

import org.joda.time.DateTime;
import org.ohmage.models.OhmageUser;

public class RectifiedMobilityDataPoint extends LifestreamsDataPoint implements IMobilityDataPoint {
	public RectifiedMobilityDataPoint(OhmageUser user, DateTime timestamp,
			TimeWindow window, BaseLifestreamsBolt generator) {
		super(user, timestamp, window, generator);
	}
	private MobilityState mode;

	public MobilityState getMode(){
		return mode;
	}
	public RectifiedMobilityDataPoint setMode(MobilityState mode){
		this.mode = mode;
		return this;
	}
}
