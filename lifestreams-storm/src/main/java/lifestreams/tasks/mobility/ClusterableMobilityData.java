package lifestreams.tasks.mobility;

import java.util.List;

import lifestreams.models.StreamRecord;
import lifestreams.models.data.MobilityData;

import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.distance.DistanceMeasure;

public class ClusterableMobilityData implements Clusterable{
	final StreamRecord<MobilityData> data;
	final int index;
	@Override
	public double[] getPoint() {
		double[] vector = {this.index};
		return vector;
	}
	public ClusterableMobilityData(int index, StreamRecord<MobilityData> data){
		this.data = data;
		this.index = index;
	}
}
