package org.apache.crunchts.pojo;

public class AbstractTS {
	
	protected String label;
	
	protected long tStart = 0;
	protected long tEnd = 0;
	
	protected int zValues = 0;
	
	public long getLength() {
		return tEnd - tStart;
	}	
	
	public String getLabel() {
		return label;
	}		

}
