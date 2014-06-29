package org.ohmage.lifestreams.models;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.joda.time.DateTime;
import org.joda.time.ReadableDateTime;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;

/**
 * This a Jackson DateTime serialization module that uses the TimeZone specified in the DateTime object / JSON String
 * by default.
 * Created by changun on 6/25/14.
 */
public class DateTimeSerializeModule extends SimpleModule {
    public final class DateTimeSerializer
            extends StdSerializer<DateTime> {

        protected DateTimeSerializer() {
            super(DateTime.class);
        }

        @Override
        public void serialize(DateTime value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonGenerationException {
            // always use the timezone of the datetime object
            DateTimeFormatter formatter = ISODateTimeFormat.dateTime().withZone(value.getZone());
            jgen.writeString(formatter.print(value));
        }
    }

    static public class DateTimeDeserializer
            extends StdScalarDeserializer<ReadableInstant> {
        private static final long serialVersionUID = 1L;

        @SuppressWarnings("unchecked")
        public DateTimeDeserializer(Class<? extends ReadableInstant> cls) {
            super(cls);
        }

        @SuppressWarnings("unchecked")
        public static <T extends ReadableInstant> JsonDeserializer<T> forType(Class<T> cls) {
            return (JsonDeserializer<T>) new DateTimeDeserializer(cls);
        }

        public DateTimeDeserializer() {
            super(DateTime.class);
        }

        ;

        @SuppressWarnings("deprecation")
        @Override
        public ReadableDateTime deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException {
            JsonToken t = jp.getCurrentToken();
            if (t == JsonToken.VALUE_STRING) {
                String str = jp.getText().trim();
                if (str.length() == 0) { // [JACKSON-360]
                    return null;
                }
                // always use the tz in the string

                return ISODateTimeFormat.dateTime().withOffsetParsed().parseDateTime(str);
            }
            // TODO: in 2.4, use 'handledType()'
            throw ctxt.mappingException(getValueClass());
        }
    }

    public DateTimeSerializeModule() {
        super("DateTimeWithTimezone");
        addDeserializer(DateTime.class, DateTimeDeserializer.forType(DateTime.class));
        addDeserializer(ReadableDateTime.class, DateTimeDeserializer.forType(ReadableDateTime.class));
        addDeserializer(ReadableInstant.class, DateTimeDeserializer.forType(ReadableInstant.class));
        addSerializer(new DateTimeSerializer());
    }


}
