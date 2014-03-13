package lifestreams.examples.activityCount;

import lifestreams.bolts.IGenerator;
import lifestreams.bolts.TimeWindow;
import lifestreams.models.data.LifestreamsData;

/**
 * @author changun This class define the data type of the output payload. It
 *         contains only one field: activityInstanceCount. Make sure to have
 *         getter method for each field to be output, so that the payload
 *         can be serialized correctly by Jackson library. @see <a
 *         href="http://wiki.
 *         fasterxml.com/JacksonInFiveMinutes#Full_Data_Binding_.28
 *         POJO.29_Example">Jackson Data Binding</a>
 * 
 *         Ohmage Stream Schema for this object is as follows 
 *         <schema> 
 *         {
 *         	"type": "object", 
 *          "doc": "geodiameter", 
 *          "fields": [ 
 *          	{ "name": "activityInstanceCount", 
 *          	  "doc":"The number of activity instances in a day",
 *         		  "type": "boolean"
 *         		} 
 *         	 ] 
 *         }
 *         </schema>
 */
public class ActivityInstanceCountData extends LifestreamsData{
	private int activityInstanceCount;

	public int getActivityInstanceCount() {
		return activityInstanceCount;
	}
	public void setActivityInstanceCount(int activityInstanceCount) {
		this.activityInstanceCount = activityInstanceCount;
	}
	public ActivityInstanceCountData(TimeWindow window, IGenerator generator) {
		super(window, generator);
	}
	
	
}