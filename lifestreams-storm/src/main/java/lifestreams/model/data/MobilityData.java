package lifestreams.model.data;

import java.io.IOException;

import lifestreams.model.MobilityState;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public class MobilityData implements IMobilityData {
	private MobilityState mode;

	public MobilityData() {

	}

	@Override
	public MobilityState getMode() {
		return mode;
	}

	@JsonDeserialize(using = MobilityStateDeserializer.class)
	public void setMode(MobilityState state) {
		this.mode = state;
	}

	public static class MobilityStateDeserializer extends
			JsonDeserializer<MobilityState> {
		@Override
		public MobilityState deserialize(JsonParser jp,
				DeserializationContext ctxt) throws IOException,
				JsonProcessingException {
			String mode = jp.getText();
			return MobilityState.valueOf(mode.toUpperCase());

		}
	}

}
