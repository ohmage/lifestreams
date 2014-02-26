package lifestreams.model;

import org.joda.time.DateTime;
import org.ohmage.models.OhmageUser;

/**
 * @author changun
 * IDataPoint interface defines the essential fileds for a data point
 */
public interface IDataPoint {
	
	// the timestamp this data point is associate with
	DateTime getTimestamp();
	// the ohmage user this data point belongs to
	OhmageUser getUser();

}
