package lifestreams.model.data;

import lifestreams.model.MobilityState;

/**
 * @author changun IMobilityPoint interface defines the essential fields for a
 *         Mobility data point
 */
public interface IMobilityData {
	MobilityState getMode();
}
