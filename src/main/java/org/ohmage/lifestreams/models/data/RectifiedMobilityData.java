package org.ohmage.lifestreams.models.data;

import org.ohmage.lifestreams.bolts.IGenerator;
import org.ohmage.lifestreams.models.MobilityState;
import org.ohmage.lifestreams.tasks.TimeWindow;

public class RectifiedMobilityData extends LifestreamsData implements
		IMobilityData {
	public RectifiedMobilityData(IGenerator generator) {
		super(generator);
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
