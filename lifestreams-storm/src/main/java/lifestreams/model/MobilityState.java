package lifestreams.model;

import co.nutrino.api.moves.impl.dto.activity.MovesActivityEnum;

public enum MobilityState {
	STILL, RUN, WALK, DRIVE, CYCLING;
	public boolean isActive() {
		if (this.equals(RUN) || this.equals(WALK) || this.equals(CYCLING)) {
			return true;
		}
		return false;
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
