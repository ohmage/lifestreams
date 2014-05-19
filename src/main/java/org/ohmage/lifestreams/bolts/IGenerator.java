package org.ohmage.lifestreams.bolts;

import java.util.Set;

public interface IGenerator {
	String getGeneratorId();
	Set<String> getSourceIds();
	String getTopologyId();
}
