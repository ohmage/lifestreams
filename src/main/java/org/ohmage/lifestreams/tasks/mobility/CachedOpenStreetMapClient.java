package org.ohmage.lifestreams.tasks.mobility;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.ohmage.lifestreams.spouts.PersistentMapFactory;
import org.slf4j.LoggerFactory;

import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;
import com.jcabi.aspects.RetryOnFailure;

import fr.dudie.nominatim.client.JsonNominatimClient;
import fr.dudie.nominatim.model.Address;

public class CachedOpenStreetMapClient {
	private JsonNominatimClient nominatimClient;
	Map<LatLng, Address> addressCache;
	@RetryOnFailure(attempts = 5, delay = 1, unit = TimeUnit.MINUTES)
	public Address getAddress(double lat, double lng) throws IOException {
		LatLng query = new LatLng(lat, lng);
		for (LatLng entry : addressCache.keySet()) {
			if (LatLngTool.distance(entry, query, LengthUnit.METER) < 30) {
				return addressCache.get(entry);
			}
		}

		// cannot find the place in the cache. Sleep a while and query the
		// OpenStreetMap.
		LoggerFactory.getLogger(this.getClass()).trace("Query {}", query);
		Address newAddress = nominatimClient.getAddress(lng, lat);
		addressCache.put(query, newAddress);
		return newAddress;
	}

	CachedOpenStreetMapClient(String email, PersistentMapFactory factory){
		
		final HttpClient httpClient = HttpClientBuilder.create().build();
		nominatimClient = new JsonNominatimClient(httpClient, email);
		addressCache = factory.getSystemWideMap(CachedOpenStreetMapClient.class.getName(), LatLng.class, Address.class);
	}
}
