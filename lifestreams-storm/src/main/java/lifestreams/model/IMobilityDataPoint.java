package lifestreams.model;

/**
 * @author changun
 * IMobilityPoint interface defines the essential fields for a Mobility data point
 */
public interface IMobilityDataPoint extends IDataPoint{
	MobilityState getMode();
	GeoLocation getLocation();
	
}
