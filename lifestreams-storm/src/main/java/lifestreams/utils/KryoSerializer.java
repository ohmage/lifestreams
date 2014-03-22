package lifestreams.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.regex.Pattern;

import lifestreams.bolts.TimeWindow;
import lifestreams.models.GeoLocation;
import lifestreams.models.MobilityState;
import lifestreams.models.StreamMetadata;
import lifestreams.models.StreamRecord;
import lifestreams.models.data.ActivityEpisode;
import lifestreams.models.data.ActivitySummaryData;
import lifestreams.models.data.GeoDiameterData;
import lifestreams.models.data.LifestreamsData;
import lifestreams.models.data.MobilityData;
import lifestreams.models.data.RectifiedMobilityData;

import org.joda.time.DateTime;
import org.ohmage.models.OhmageServer;
import org.ohmage.models.OhmageUser;

import backtype.storm.Config;
import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.shaded.org.objenesis.strategy.StdInstantiatorStrategy;

import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.BitSetSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyMapSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptySetSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonMapSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonSetSerializer;
import de.javakaffee.kryoserializers.EnumMapSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.RegexSerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;

public class KryoSerializer {

	public static Kryo getInstance() {
		Kryo kryo = new Kryo();

		// 
		// kryo.setRegistrationRequired(false);
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


		
		// joda datetime
		kryo.register(DateTime.class, new JodaDateTimeSerializer());

		kryo.register(ArrayList.class);
		kryo.register(LinkedList.class);
		kryo.register(HashMap.class);
		kryo.register(HashSet.class);

		kryo.register(OhmageUser.class);
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
		kryo.register(LifestreamsData.class);
		kryo.register(MobilityData.class);
		kryo.register(RectifiedMobilityData.class);
		kryo.register(MovesSegment.class);
		return kryo;
	}
	public static void setRegistrationsForStormConfig(Config config){
		Kryo kryo = getInstance();
		// merge all the registered classes and serializers to the Storm Config.
		for(int i=0; i<kryo.getNextRegistrationId(); i++){
			if(kryo.getRegistration(i) != null){
				Registration reg = kryo.getRegistration(i);
				if(reg.getSerializer() != null){
					config.registerSerialization(reg.getType(), reg.getSerializer().getClass());
				}
				else{
					config.registerSerialization(reg.getType());
				}
			}
		}
	}
}
