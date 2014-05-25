package org.ohmage.lifestreams.models.data;

import java.util.Set;

import org.ohmage.lifestreams.bolts.IGenerator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LifestreamsData {

	protected static class GeneratorInfo {
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

	protected GeneratorInfo generator;

	public LifestreamsData() {
		super();
	}
	public LifestreamsData(IGenerator generator) {
		// populate info for generator
		this.generator = new GeneratorInfo();
		this.generator.setComponentId(generator.getGeneratorId());
		this.generator.setTopologyId(generator.getTopologyId());
		this.generator.setSourceIds(generator.getSourceIds());
	}

	public String toString() {
		return String.format("Type:%s Component:%s"
					, this.getClass().getName()
					, this.getGenerator().getComponentId());
	}

	@JsonProperty
	public GeneratorInfo getGenerator() {
		return generator;
	}

	@JsonIgnore
	public void setGenerator(GeneratorInfo generator) {
		this.generator = generator;
	}

}