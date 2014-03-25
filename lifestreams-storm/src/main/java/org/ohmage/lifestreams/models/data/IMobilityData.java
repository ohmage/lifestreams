package org.ohmage.lifestreams.models.data;

import org.ohmage.lifestreams.models.MobilityState;

/**
 * @author changun IMobilityPoint interface defines the essential fields for a
 *         Mobility data point
 */
public interface IMobilityData {
	MobilityState getMode();
}
