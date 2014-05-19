package org.ohmage.lifestreams.models.data;

public class WiFi{
	final private Long bssid;
	public WiFi(String bssid){
		// convert BSSID to Long to save space
		this.bssid = Long.decode("#"+bssid.toUpperCase().replace(":", ""));
	}
	public boolean equals(Object arg0) {
		if(arg0 instanceof WiFi){
			return bssid.equals(((WiFi)arg0).bssid);
		}
		else{
			return false;
		}
	}
	public int hashCode() {
		return bssid.hashCode();
	}
	public String toString(){
		return bssid.toString();
	}
}