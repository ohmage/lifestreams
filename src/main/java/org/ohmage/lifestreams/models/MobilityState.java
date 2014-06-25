package org.ohmage.lifestreams.models;

import co.nutrino.api.moves.impl.dto.activity.MovesActivityEnum;

public enum MobilityState {
	STILL, RUN, WALK, DRIVE, CYCLING, UNKNOWN;
	public boolean isActive() {
        return this.equals(RUN) || this.equals(WALK) || this.equals(CYCLING);
    }

	static public MobilityState fromMovesActivity(MovesActivityEnum moves) {
		if (moves.equals(MovesActivityEnum.Cycling))
			return CYCLING;
		else if (moves.equals(MovesActivityEnum.Running))
			return RUN;
		else if (moves.equals(MovesActivityEnum.Walking))
			return WALK;
		else if (moves.equals(MovesActivityEnum.Trip))
			return DRIVE;
		else
			throw new RuntimeException();
	}
}
