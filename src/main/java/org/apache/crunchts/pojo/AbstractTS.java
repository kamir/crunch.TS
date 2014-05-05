package org.apache.crunchts.pojo;

public class AbstractTS {
	
	String label;
	
	long tStart = 0;
	long tEnd = 0;
	
	int zValues = 0;
	
	public long getLength() {
		return tEnd - tStart;
	}	
	
	public String getLabel() {
		return label;
	}	

}
