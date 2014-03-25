package org.ohmage.lifestreams.models.data;

import org.ohmage.lifestreams.bolts.IGenerator;
import org.ohmage.lifestreams.bolts.TimeWindow;
import org.ohmage.lifestreams.models.MobilityState;

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
