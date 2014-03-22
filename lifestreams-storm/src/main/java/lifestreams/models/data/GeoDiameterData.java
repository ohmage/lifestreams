package lifestreams.models.data;

import lifestreams.bolts.IGenerator;
import lifestreams.bolts.TimeWindow;
import lifestreams.models.GeoLocation;

public class GeoDiameterData extends LifestreamsData {

	double diameter;
	GeoLocation earlierPointOnDiameter;
	GeoLocation laterPointOnDiameter;

	public double getGeoDiameterInMiles() {
		return diameter;
	}

	public GeoDiameterData setDiameter(double diameter) {
		this.diameter = diameter;
		return this;
	}

	public GeoLocation getEarlierPointOnDiameter() {
		return earlierPointOnDiameter;
	}

	public GeoDiameterData setEarlierPointOnDiameter(
			GeoLocation earlierPointOnDiameter) {
		this.earlierPointOnDiameter = earlierPointOnDiameter;
		return this;
	}

	public GeoLocation getLaterPointOnDiameter() {
		return laterPointOnDiameter;
	}

	public GeoDiameterData setLaterPointOnDiameter(
			GeoLocation laterPointOnDiameter) {
		this.laterPointOnDiameter = laterPointOnDiameter;
		return this;
	}

	public GeoDiameterData(TimeWindow window, IGenerator generator) {
		super(window, generator);
	}

}
