package lifestreams.model;


public enum MobilityState {
    STILL, RUN, WALK, DRIVE, CYCLING;
    public boolean isActive(){
    	if(this.equals(RUN) || this.equals(WALK) || this.equals(CYCLING)){
    		return true;
    	}
    	return false;
    }
}
