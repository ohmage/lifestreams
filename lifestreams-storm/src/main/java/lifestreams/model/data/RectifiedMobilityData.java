package lifestreams.model.data;

import lifestreams.bolt.IGenerator;
import lifestreams.model.MobilityState;
import lifestreams.utils.TimeWindow;

public class RectifiedMobilityData extends LifestreamsData implements
		IMobilityData {
	public RectifiedMobilityData(TimeWindow window, IGenerator generator) {
		super(window, generator);
	}

	private MobilityState mode;

	@Override
	public MobilityState getMode() {
		return mode;
	}

	public RectifiedMobilityData setMode(MobilityState mode) {
		this.mode = mode;
		return this;
	}
}
