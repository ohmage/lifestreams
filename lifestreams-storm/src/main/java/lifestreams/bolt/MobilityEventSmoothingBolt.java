package lifestreams.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import lifestreams.model.IDataPoint;
import lifestreams.model.MobilityDataPoint;
import lifestreams.model.MobilityState;
import lifestreams.model.RectifiedMobilityDataPoint;

import org.ohmage.models.OhmageUser;

import lifestreams.utils.TimeWindow;

import org.joda.time.Duration;
import org.joda.time.base.BaseSingleFieldPeriod;

import state.UserState;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import be.ac.ulg.montefiore.run.jahmm.Hmm;
import be.ac.ulg.montefiore.run.jahmm.ObservationDiscrete;
import be.ac.ulg.montefiore.run.jahmm.ObservationInteger;
import be.ac.ulg.montefiore.run.jahmm.OpdfDiscrete;
import be.ac.ulg.montefiore.run.jahmm.OpdfDiscreteFactory;
import be.ac.ulg.montefiore.run.jahmm.OpdfIntegerFactory;

public class MobilityEventSmoothingBolt extends LifestreamsBolt{
	private static final String MOBILITY_DATA_POINTS = "MOBILITY_DATA_POINTS";

	public MobilityEventSmoothingBolt(BaseSingleFieldPeriod period) {
		super(period);
		// TODO Auto-generated constructor stub
	}

	Hmm<ObservationDiscrete<MobilityState>> hmmModel;

	@Override
	public void prepare(Map stormConf, TopologyContext context){
		super.prepare(stormConf, context);
		hmmModel = createHmmModel();
		
	}
	
	public static Hmm<ObservationDiscrete<MobilityState>>  createHmmModel(){
		OpdfDiscreteFactory<MobilityState> factory = new OpdfDiscreteFactory<MobilityState>(MobilityState.class);
		Hmm<ObservationDiscrete<MobilityState>> hmm = new Hmm<ObservationDiscrete<MobilityState>>(MobilityState.values().length , factory);
		// Assume we will never have CYCLING state
		for(MobilityState state: MobilityState.values()){
			if(state.equals(MobilityState.CYCLING))
				hmm.setPi(state.ordinal(), 0);
			else
				hmm.setPi(state.ordinal(), 1.0/(MobilityState.values().length-1));
		}
		
		hmm.setOpdf(MobilityState.STILL.ordinal(), 
				new OpdfDiscrete<MobilityState>(MobilityState.class, new double[] { 0.70, 0.075, 0.075, 0.15, 0 }));
		hmm.setOpdf(MobilityState.RUN.ordinal(), 
				new OpdfDiscrete<MobilityState>(MobilityState.class, new double[] { 0.01, 0.70, 0.14, 0.15, 0 }));
		hmm.setOpdf(MobilityState.WALK.ordinal(), 
				new OpdfDiscrete<MobilityState>(MobilityState.class, new double[] { 0.01, 0.14, 0.70, 0.15, 0 }));
		hmm.setOpdf(MobilityState.DRIVE.ordinal(), 
				new OpdfDiscrete<MobilityState>(MobilityState.class, new double[] { 0.28, 0.01, 0.01, 0.70, 0 }));
		
		hmm.setAij(MobilityState.STILL.ordinal(), MobilityState.STILL.ordinal(), 0.70);
		hmm.setAij(MobilityState.STILL.ordinal(), MobilityState.RUN.ordinal(), 0.14);
		hmm.setAij(MobilityState.STILL.ordinal(), MobilityState.WALK.ordinal(), 0.15);
		hmm.setAij(MobilityState.STILL.ordinal(), MobilityState.DRIVE.ordinal(), 0.01);
		hmm.setAij(MobilityState.STILL.ordinal(), MobilityState.CYCLING.ordinal(), 0.00);
		
		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.STILL.ordinal(), 0.25);
		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.RUN.ordinal(), 0.49);
		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.WALK.ordinal(), 0.25);
		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.DRIVE.ordinal(), 0.01);
		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.CYCLING.ordinal(), 0.00);
		
		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.STILL.ordinal(), 0.15);
		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.RUN.ordinal(), 0.15);
		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.WALK.ordinal(), 0.69);
		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.DRIVE.ordinal(), 0.01);
		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.CYCLING.ordinal(), 0.00);
		
		hmm.setAij(MobilityState.DRIVE.ordinal(), MobilityState.STILL.ordinal(), 0.09);
		hmm.setAij(MobilityState.DRIVE.ordinal(), MobilityState.RUN.ordinal(), 0.01);
		hmm.setAij(MobilityState.DRIVE.ordinal(), MobilityState.WALK.ordinal(), 0.20);
		hmm.setAij(MobilityState.DRIVE.ordinal(), MobilityState.DRIVE.ordinal(), 0.70);
		hmm.setAij(MobilityState.DRIVE.ordinal(), MobilityState.CYCLING.ordinal(), 0.00);
		return(hmm);
	}
	@SuppressWarnings("unchecked")
	private List<MobilityDataPoint> getMobilityDataPoints(UserState state){
		return (List<MobilityDataPoint>) state.get(MOBILITY_DATA_POINTS);
		
	}
	@Override
	protected void newUser(OhmageUser user, UserState state){
		super.newUser(user, state);
		List<MobilityDataPoint> data_cache= new LinkedList<MobilityDataPoint>();
		state.put(MOBILITY_DATA_POINTS, data_cache);
	}
	@Override
	protected boolean executeDataPoint(OhmageUser user, IDataPoint dp,
			UserState state, TimeWindow window, BasicOutputCollector collector) {
		getMobilityDataPoints(state).add((MobilityDataPoint) dp);
		return false;
	}
	@Override
	protected void finishWindow(OhmageUser user, UserState state,
			TimeWindow window, BasicOutputCollector collector) {
		snapshotWindow(user, state, window, collector);
		getMobilityDataPoints(state).clear();
		
	}
	@Override
	protected void snapshotWindow(OhmageUser user, UserState state,TimeWindow window, 
			BasicOutputCollector collector) {
		List<MobilityDataPoint> data = getMobilityDataPoints(state);
		
		// create a list of observations (i.e. mobility states)
		List<ObservationDiscrete <MobilityState>> observations = new ArrayList<ObservationDiscrete <MobilityState>> ();
		for(IDataPoint dp: data){
			MobilityDataPoint mdp = (MobilityDataPoint)dp;
			observations.add(new ObservationDiscrete <MobilityState>(mdp.getMode()));
		}
		// compute the most likely state given the hmm model
		int[] inferredStates= hmmModel.mostLikelyStateSequence(observations);
		
		// emit the data with the new states
		for(int i=0; i<inferredStates.length; i++){
			MobilityState curState = MobilityState.values()[inferredStates[i]];
			// create a new Mobility data point
			RectifiedMobilityDataPoint rectifiedDp = new RectifiedMobilityDataPoint(user,
					data.get(i).getTimestamp(), window, this).setMode(curState);
			rectifiedDp.setLocation(data.get(i).getLocation());
			logger.info("raw {} rectified {}", data.get(i).getMode(), curState);
			this.emit(rectifiedDp, collector);
			
		}

		
	}


}
