package org.ohmage.lifestreams;

import backtype.storm.Config;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.codec.binary.Base64;
import org.ohmage.lifestreams.utils.KryoSerializer;

import java.util.Map;

/**
 * This class contains lifestreams topology specific configuration.
 *
 * @author changun
 */
public class LifestreamsConfig {
    // whether to write back the processed data to ohmage
    public static final String DRYRUN_WITHOUT_UPLOADING = "lifestreams.dryrun";
    // whether to write back the processed data to ohmage
    public static final String LIFESTREAMS_REQUESTER = "lifestreams.requester";
    // whether to write back the processed data to ohmage
    public static final String LIFESTREAMS_REQUESTEES = "lifestreams.requestees";
    // persistent map store to keep computation state
    public static final String MAP_STORE_INSTANCE = "lifestreams.mapStore";
    // persistent stream store to write output data
    public static final String STREAM_STORE_INSTANCE = "lifestreams.streamStore";

    public static void serializeAndPutObject(Config config, String key, Object obj) {
        Output output = new Output(10 * 1024);
        KryoSerializer.getInstance().writeClassAndObject(output, obj);
        config.put(key, Base64.encodeBase64String(output.getBuffer()));
    }

    public static Object getAndDeserializeObject(Map config, String key) {
        byte[] bin = org.apache.commons.codec.binary.Base64.decodeBase64((String) config.get(key));
        return KryoSerializer.getInstance().readClassAndObject(new Input(bin));
    }
}
