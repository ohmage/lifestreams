package org.ohmage.lifestreams.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.ohmage.lifestreams.models.GeoLocation;
import org.ohmage.lifestreams.models.MobilityState;
import org.ohmage.lifestreams.models.StreamMetadata;
import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.models.data.ActivityEpisode;
import org.ohmage.lifestreams.models.data.ActivitySummaryData;
import org.ohmage.lifestreams.models.data.GeoDiameterData;
import org.ohmage.lifestreams.models.data.MobilityData;
import org.ohmage.lifestreams.models.data.RectifiedMobilityData;
import org.ohmage.lifestreams.models.data.TimeWindowData;
import org.ohmage.lifestreams.tasks.TimeWindow;
import org.ohmage.models.OhmageServer;
import org.ohmage.models.OhmageUser;

import backtype.storm.serialization.IKryoFactory;
import co.nutrino.api.moves.impl.dto.storyline.MovesSegment;

import com.esotericsoftware.kryo.Kryo;
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

public class KryoSerializer  implements IKryoFactory {

	public static Kryo getInstance()  {
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
}
