package org.ohmage.lifestreams.utils;

import backtype.storm.serialization.IKryoFactory;
import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.shaded.org.objenesis.strategy.StdInstantiatorStrategy;
import de.javakaffee.kryoserializers.*;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.MobilityState;
import org.ohmage.lifestreams.models.StreamMetadata;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.*;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.ohmage.models.Ohmage20User;
import org.ohmage.models.OhmageServer;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class KryoSerializer implements IKryoFactory {

    public static Kryo getInstance() {
        Kryo kryo = new Kryo();

        //
        kryo.setRegistrationRequired(false);
        kryo.setReferences(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());

        // some common classes and their corresponding serializer
        kryo.register(Arrays.asList("").getClass(),
                new ArraysAsListSerializer());
        kryo.register(Collections.EMPTY_LIST.getClass(),
                new CollectionsEmptyListSerializer());
        kryo.register(Collections.EMPTY_MAP.getClass(),
                new CollectionsEmptyMapSerializer());
        kryo.register(Collections.EMPTY_SET.getClass(),
                new CollectionsEmptySetSerializer());
        kryo.register(Collections.singletonList("").getClass(),
                new CollectionsSingletonListSerializer());
        kryo.register(Collections.singleton("").getClass(),
                new CollectionsSingletonSetSerializer());
        kryo.register(Collections.singletonMap("", "").getClass(),
                new CollectionsSingletonMapSerializer());
        kryo.register(GregorianCalendar.class,
                new GregorianCalendarSerializer());
        kryo.register(EnumMap.class, new EnumMapSerializer());
        kryo.register(BitSet.class, new BitSetSerializer());
        kryo.register(Pattern.class, new RegexSerializer());

        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        SynchronizedCollectionsSerializer.registerSerializers(kryo);
        // Kryo can't not serialize IndifferentAccessMap in storm, use the default java serializer


        // joda datetime
        kryo.register(DateTime.class, new JodaDateTimeSerializer());

        kryo.register(ArrayList.class);
        kryo.register(LinkedList.class);
        kryo.register(HashMap.class);
        kryo.register(HashSet.class);

        kryo.register(Ohmage20User.class);
        kryo.register(OhmageServer.class);

        kryo.register(TimeWindow.class);
        kryo.register(PendingBuffer.class);

		/* base stream record models */
        kryo.register(GeoLocation.class);
        kryo.register(MobilityState.class);
        kryo.register(StreamMetadata.class);
        kryo.register(StreamRecord.class);

		/* stream data models */
        kryo.register(MobilityData.class);
        kryo.register(GeoDiameterData.class);
        kryo.register(ActivityEpisode.class);
        kryo.register(ActivitySummaryData.class);
        kryo.register(TimeWindowData.class);
        kryo.register(MobilityData.class);
        kryo.register(RectifiedMobilityData.class);
        kryo.register(MovesSegment.class);
        return kryo;
    }

    @Override
    public Kryo getKryo(Map conf) {
        // TODO Auto-generated method stub
        return getInstance();
    }

    @Override
    public void preRegister(Kryo k, Map conf) {
        // TODO Auto-generated method stub

    }

    @Override
    public void postRegister(Kryo k, Map conf) {
        // TODO Auto-generated method stub

    }

    @Override
    public void postDecorate(Kryo k, Map conf) {
        // TODO Auto-generated method stub

    }

    static public byte[] getBytes(Object obj, Kryo kryo) {
        ByteArrayOutputStream byteArrayOutputStream =
                new ByteArrayOutputStream(1024 * 1024);
        DeflaterOutputStream deflaterOutputStream =
                new DeflaterOutputStream(byteArrayOutputStream);
        Output valOutput = new Output(deflaterOutputStream);

        kryo.writeClassAndObject(valOutput, obj);
        valOutput.close();
        return byteArrayOutputStream.toByteArray();
    }

    static public <T> T toObject(byte[] bytes, Class<T> c, Kryo kryo) {
        Input in = new Input(new InflaterInputStream(new ByteArrayInputStream(bytes)));
        Object obj = kryo.readClassAndObject(in);
        return (T) obj;

    }
}
