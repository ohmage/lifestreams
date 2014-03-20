package lifestreams;

public class LifestreamsConfig {
	// whether to write back the processed data to ohmage
	public static String DRYRUN_WITHOUT_UPLOADING = "lifestreams.dryrun";
	// whether to write output to a local redis store 
	public static String OUTPUT_TO_LOCAL_REDIS = "lifestreams.output.to.redis";
	// whether to store the computation state in the local redis store 
	public static String ENABLE_STATEFUL_FUNCTION = "lifestreams.enable_stateful";
}
