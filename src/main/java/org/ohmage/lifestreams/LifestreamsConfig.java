package org.ohmage.lifestreams;

/**
 * This class contains lifestreams topology specific configuration.
 * @author changun
 *
 */
public class LifestreamsConfig {
	// whether to write back the processed data to ohmage
	public static String DRYRUN_WITHOUT_UPLOADING = "lifestreams.dryrun";
	// whether to store the computation state in the local redis store 
	public static String ENABLE_STATEFUL_FUNCTION = "lifestreams.enable_stateful";
}
