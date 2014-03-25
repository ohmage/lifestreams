package org.ohmage.lifestreams.models.data;

public class WiFi{
	final private Long bssid;
	public WiFi(String bssid){
		this.bssid = Long.decode("#"+bssid.toUpperCase().replace(":", ""));
	}
	public boolean equals(Object arg0) {
		return bssid.equals(arg0.toString());
	}
	public int hashCode() {
		return bssid.hashCode();
	}
	public String toString(){
		return bssid.toString();
	}
}