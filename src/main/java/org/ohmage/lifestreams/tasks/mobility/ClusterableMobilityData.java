package org.ohmage.lifestreams.tasks.mobility;

import java.util.List;

import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.MobilityData;

public class ClusterableMobilityData implements Clusterable{
	final StreamRecord<MobilityData> data;
	final int index;
	int place;
	@Override
	public double[] getPoint() {
		double[] vector = {this.index};
		return vector;
	}

	public int getPlace() {
		return place;
	}

	public void setPlace(int place) {
		this.place = place;
	}

	public StreamRecord<MobilityData> getData() {
		return data;
	}

	public int getIndex() {
		return index;
	}

	public ClusterableMobilityData(int index, StreamRecord<MobilityData> data){
		this.data = data;
		this.index = index;
	}
}
