package lifestreams.model;

import lifestreams.bolt.BaseLifestreamsBolt;
import lifestreams.utils.TimeWindow;

import org.joda.time.DateTime;
import org.ohmage.models.OhmageUser;

public class GeoDiameterDataPoint extends LifestreamsDataPoint {

	public double getDiameterInMeter() {
		return diameterInKM;
	}

	public GeoDiameterDataPoint setDiameterInKM(double diameterInKM) {
		this.diameterInKM = diameterInKM;
		return this;
	}

	public MobilityDataPoint getEalierPointOnDiameter() {
		return ealierPointOnDiameter;
	}

	public GeoDiameterDataPoint setEalierPointOnDiameter(MobilityDataPoint ealierPointOnDiameter) {
		this.ealierPointOnDiameter = ealierPointOnDiameter;
		return this;
	}

	public MobilityDataPoint getLaterPointOnDiameter() {
		return laterPointOnDiameter;
	}

	public GeoDiameterDataPoint setLaterPointOnDiameter(MobilityDataPoint laterPointOnDiameter) {
		this.laterPointOnDiameter = laterPointOnDiameter;
		return this;
	}

	double diameterInKM;
	MobilityDataPoint ealierPointOnDiameter;
	MobilityDataPoint laterPointOnDiameter;
	
	public GeoDiameterDataPoint(OhmageUser user, TimeWindow window, BaseLifestreamsBolt generator) {
		super(user, window.getLastInstant(), window, generator);
	}

}
