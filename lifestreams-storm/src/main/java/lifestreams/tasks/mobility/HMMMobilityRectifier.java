package lifestreams.tasks.mobility;

import java.util.ArrayList;
import java.util.List;

import lifestreams.bolts.TimeWindow;
import lifestreams.bolts.TimeWindowBolt;
import lifestreams.models.MobilityState;
import lifestreams.models.StreamRecord;
import lifestreams.models.data.IMobilityData;
import lifestreams.models.data.RectifiedMobilityData;
import lifestreams.tasks.SimpleTask;

import org.ohmage.models.OhmageUser;

import be.ac.ulg.montefiore.run.jahmm.Hmm;
import be.ac.ulg.montefiore.run.jahmm.ObservationDiscrete;
import be.ac.ulg.montefiore.run.jahmm.OpdfDiscrete;
import be.ac.ulg.montefiore.run.jahmm.OpdfDiscreteFactory;

/**
 * @author changun This task uses a Hidden Markov Chain model to correct the
 *         possible errors in the Mobility classification. The process is
 *         required as the Mobility classification is subject to short term
 *         signal variations (for example, variation in ambient Wi-Fi signals,)
 *         and sometimes mis-classify the mobility states. We correct these
 *         possible errors by considering a long series of data points in a
 *         whole (e.g. one day's data) and incorporating the common error
 *         patterns of Mobility classifier in the HMM model. For example,
 *         Mobility classifier tend to mis-classify Still, Walking, or Running
 *         as Drive, or Drive as Still. The HMM modle is able to correct those
 *         errors.
 */
public class HMMMobilityRectifier extends SimpleTask<IMobilityData> {
	Hmm<ObservationDiscrete<MobilityState>> hmmModel;
	List<StreamRecord<IMobilityData>> data = new ArrayList<StreamRecord<IMobilityData>>();

	public void init(OhmageUser user, TimeWindowBolt bolt) {
		super.init(user, bolt);
		this.hmmModel = createHmmModel();
	}

	@Override
	public void executeDataPoint(StreamRecord<IMobilityData> dp,
			TimeWindow window) {
		data.add(dp);
	}

	public void performRectificationAndEmitRecords(TimeWindow window, boolean isSanpshot){
		// create a list of observations (i.e. mobility states)
		List<ObservationDiscrete<MobilityState>> observations = new ArrayList<ObservationDiscrete<MobilityState>>();
		for (StreamRecord<IMobilityData> dp : data) {
			IMobilityData mdp = dp.d();
			observations.add(new ObservationDiscrete<MobilityState>(mdp
					.getMode()));
		}
		// compute the most likely state given the hmm model
		int[] inferredStates = hmmModel.mostLikelyStateSequence(observations);

		// emit the data with the new states
		for (int i = 0; i < inferredStates.length; i++) {
			MobilityState curState = MobilityState.values()[inferredStates[i]];
			// create a new Mobility data point
			RectifiedMobilityData rectifiedDp = new RectifiedMobilityData(window, this).setMode(curState);
			this.createRecord()
					.setData(rectifiedDp)
					.setLocation(data.get(i).getLocation())
					.setTimestamp(data.get(i).getTimestamp())
					.setIsSnapshot(isSanpshot)
					.emit();
		}

	}
	@Override
	public void finishWindow(TimeWindow window) {
		performRectificationAndEmitRecords(window, false);
		data.clear();
	}

	@Override
	public void snapshotWindow(TimeWindow window) {
		performRectificationAndEmitRecords(window, true);
	}

	public static Hmm<ObservationDiscrete<MobilityState>> createHmmModel() {
		OpdfDiscreteFactory<MobilityState> factory = new OpdfDiscreteFactory<MobilityState>(
				MobilityState.class);
		Hmm<ObservationDiscrete<MobilityState>> hmm = new Hmm<ObservationDiscrete<MobilityState>>(
				MobilityState.values().length, factory);
		// Assume we will never have CYCLING state
		for (MobilityState state : MobilityState.values()) {
			if (state.equals(MobilityState.CYCLING)) {
				hmm.setPi(state.ordinal(), 0);
			} else {
				hmm.setPi(state.ordinal(),
						1.0 / (MobilityState.values().length - 1));
			}
		}

		hmm.setOpdf(MobilityState.STILL.ordinal(),
				new OpdfDiscrete<MobilityState>(MobilityState.class,
						new double[] { 0.70, 0.075, 0.075, 0.15, 0 }));
		hmm.setOpdf(MobilityState.RUN.ordinal(),
				new OpdfDiscrete<MobilityState>(MobilityState.class,
						new double[] { 0.01, 0.70, 0.14, 0.15, 0 }));
		hmm.setOpdf(MobilityState.WALK.ordinal(),
				new OpdfDiscrete<MobilityState>(MobilityState.class,
						new double[] { 0.01, 0.14, 0.70, 0.15, 0 }));
		hmm.setOpdf(MobilityState.DRIVE.ordinal(),
				new OpdfDiscrete<MobilityState>(MobilityState.class,
						new double[] { 0.28, 0.01, 0.01, 0.70, 0 }));

		hmm.setAij(MobilityState.STILL.ordinal(),
				MobilityState.STILL.ordinal(), 0.70);
		hmm.setAij(MobilityState.STILL.ordinal(), MobilityState.RUN.ordinal(),
				0.14);
		hmm.setAij(MobilityState.STILL.ordinal(), MobilityState.WALK.ordinal(),
				0.15);
		hmm.setAij(MobilityState.STILL.ordinal(),
				MobilityState.DRIVE.ordinal(), 0.01);
		hmm.setAij(MobilityState.STILL.ordinal(),
				MobilityState.CYCLING.ordinal(), 0.00);

		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.STILL.ordinal(),
				0.25);
		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.RUN.ordinal(),
				0.49);
		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.WALK.ordinal(),
				0.25);
		hmm.setAij(MobilityState.RUN.ordinal(), MobilityState.DRIVE.ordinal(),
				0.01);
		hmm.setAij(MobilityState.RUN.ordinal(),
				MobilityState.CYCLING.ordinal(), 0.00);

		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.STILL.ordinal(),
				0.15);
		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.RUN.ordinal(),
				0.15);
		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.WALK.ordinal(),
				0.69);
		hmm.setAij(MobilityState.WALK.ordinal(), MobilityState.DRIVE.ordinal(),
				0.01);
		hmm.setAij(MobilityState.WALK.ordinal(),
				MobilityState.CYCLING.ordinal(), 0.00);

		hmm.setAij(MobilityState.DRIVE.ordinal(),
				MobilityState.STILL.ordinal(), 0.09);
		hmm.setAij(MobilityState.DRIVE.ordinal(), MobilityState.RUN.ordinal(),
				0.01);
		hmm.setAij(MobilityState.DRIVE.ordinal(), MobilityState.WALK.ordinal(),
				0.20);
		hmm.setAij(MobilityState.DRIVE.ordinal(),
				MobilityState.DRIVE.ordinal(), 0.70);
		hmm.setAij(MobilityState.DRIVE.ordinal(),
				MobilityState.CYCLING.ordinal(), 0.00);
		return (hmm);
	}
}
