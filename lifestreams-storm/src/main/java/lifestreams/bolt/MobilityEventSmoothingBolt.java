package lifestreams.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import lifestreams.model.DataPoint;
import lifestreams.model.MobilityDataPoint;
import lifestreams.model.OhmageUser;

import org.joda.time.Duration;
import org.joda.time.base.BaseSingleFieldPeriod;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import be.ac.ulg.montefiore.run.jahmm.Hmm;
import be.ac.ulg.montefiore.run.jahmm.ObservationDiscrete;
import be.ac.ulg.montefiore.run.jahmm.ObservationInteger;
import be.ac.ulg.montefiore.run.jahmm.OpdfDiscrete;
import be.ac.ulg.montefiore.run.jahmm.OpdfDiscreteFactory;
import be.ac.ulg.montefiore.run.jahmm.OpdfIntegerFactory;

public class MobilityEventSmoothingBolt extends BasicLifestreamsBolt{
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
	@Override
	protected void executeBatch(OhmageUser user, List<DataPoint> data, BasicOutputCollector collector) {
		// create a list of observations (i.e. mobility states)
		List<ObservationDiscrete <MobilityState>> observations = new ArrayList<ObservationDiscrete <MobilityState>> ();
		for(DataPoint dp: data){
			MobilityDataPoint mdp = (MobilityDataPoint)dp;
			observations.add(new ObservationDiscrete <MobilityState>(mdp.getState()));
		}
		// compute the most likely state given the hmm model
		int[] inferredStates= hmmModel.mostLikelyStateSequence(observations);
		
		// emit the data with the new states
		for(int i=0; i<inferredStates.length; i++){
			MobilityState state = MobilityState.values()[inferredStates[i]];
			((MobilityDataPoint)data.get(i)).setState(state);
			this.emit(data.get(i), collector);
			/*
			if(state != observations.get(i).value){
				System.out.printf("*%s %s %s\n", observations.get(i).value, state, data.get(i).getTimestamp());
			}
			else{
				System.out.printf("%s %s %s\n", observations.get(i).value, state, data.get(i).getTimestamp());
			}*/
		}
	}
	
	public static Hmm<ObservationDiscrete<MobilityState>>  createHmmModel(){
		OpdfDiscreteFactory<MobilityState> factory = new OpdfDiscreteFactory<MobilityState>(MobilityState.class);
		Hmm<ObservationDiscrete<MobilityState>> hmm = new Hmm<ObservationDiscrete<MobilityState>>(4 , factory);
		for(MobilityState state: MobilityState.values()){
			hmm.setPi(state.ordinal(), 1.0/(double)MobilityState.values().length);
		}
		
		hmm.setOpdf(MobilityState.STILL.ordinal(), 
				new OpdfDiscrete<MobilityState>(MobilityState.class, new double[] { 0.83, 0.01, 0.01, 0.15 }));
		hmm.setOpdf(MobilityState.RUN.ordinal(), 
				new OpdfDiscrete<MobilityState>(MobilityState.class, new double[] { 0.01, 0.70, 0.14, 0.15 }));
		hmm.setOpdf(MobilityState.WALK.ordinal(), 
				new OpdfDiscrete<MobilityState>(MobilityState.class, new double[] { 0.01, 0.14, 0.70, 0.15 }));
		hmm.setOpdf(MobilityState.DRIVE.ordinal(), 
				new OpdfDiscrete<MobilityState>(MobilityState.class, new double[] { 0.28, 0.01, 0.01, 0.70 }));
		
		hmm.setAij(MobilityState.STILL.ordinal(), MobilityState.STILL.ordinal(), 0.70);
		hmm.setAij(MobilityState.STILL.ordinal(), MobilityState.RUN.ordinal(), 0.14);
		hmm.setAij(MobilityState.STILL.ordinal(), MobilityState.WALK.ordinal(), 0.15);
		hmm.setAij(MobilityState.STILL.ordinal(), MobilityState.DRIVE.ordinal(), 0.01);
		
		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.STILL.ordinal(), 0.25);
		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.RUN.ordinal(), 0.49);
		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.WALK.ordinal(), 0.25);
		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.DRIVE.ordinal(), 0.01);
		
		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.STILL.ordinal(), 0.15);
		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.RUN.ordinal(), 0.15);
		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.WALK.ordinal(), 0.69);
		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.DRIVE.ordinal(), 0.01);
		
		hmm.setAij(MobilityState.DRIVE.ordinal(), MobilityState.STILL.ordinal(), 0.09);
		hmm.setAij(MobilityState.DRIVE.ordinal(), MobilityState.RUN.ordinal(), 0.01);
		hmm.setAij(MobilityState.DRIVE.ordinal(), MobilityState.WALK.ordinal(), 0.20);
		hmm.setAij(MobilityState.DRIVE.ordinal(), MobilityState.DRIVE.ordinal(), 0.70);
		return(hmm);
	}

}
