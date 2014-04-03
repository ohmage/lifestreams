package org.ohmage.lifestreams.models.data;

import java.util.Set;

import org.ohmage.lifestreams.bolts.IGenerator;
import org.ohmage.lifestreams.bolts.TimeWindow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LifestreamsData {
	static class GeneratorInfo {
		public String getTopologyId() {
			return topologyId;
		}

		public void setTopologyId(String topologyId) {
			this.topologyId = topologyId;
		}

		public String getComponentId() {
			return componentId;
		}

		public void setComponentId(String componentId) {
			this.componentId = componentId;
		}
		public Set<String> getSourceIds() {
			return sourceIds;
		}

		public void setSourceIds(Set<String> sourceIds) {
			this.sourceIds = sourceIds;
		}
		String topologyId;
		String componentId;
		Set<String> sourceIds;

	}

	boolean isSnapshot;
	GeneratorInfo generator;
	TimeWindow timeWindow;

	public TimeWindow getTimeWindow() {
		return timeWindow;
	}

	public String toString(){
		return String.format("Type:%s Component:%s TimeWindow:%s-%s"
					, this.getClass().getName()
					, this.getGenerator().getComponentId()
					, this.getTimeWindow().getTimeWindowBeginTime()
					, this.getTimeWindow().getTimeWindowEndTime());
	}
	public boolean isSnapshot() {
		return isSnapshot;
	}

	public void setSnapshot(boolean isSnapshot) {
		this.isSnapshot = isSnapshot;
	}

	@JsonProperty
	public GeneratorInfo getGenerator() {
		return generator;
	}

	@JsonIgnore
	// ignore this when deserialization
	public void setGenerator(GeneratorInfo generator) {
		this.generator = generator;
	}

	public LifestreamsData(TimeWindow window, IGenerator generator) {
		// populate info for generator
		this.generator = new GeneratorInfo();
		this.generator.setComponentId(generator.getGeneratorId());
		this.generator.setTopologyId(generator.getTopologyId());
		this.generator.setSourceIds(generator.getSourceIds());
		// populate the timewindow this data point cover
		this.timeWindow = window;
	}

}
