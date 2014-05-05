package org.apache.crunchts.ptypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * This stub class is not used at the moment.
 * 
 * @author training
 *
 */
public class EventTSWritable implements Writable {

	
	public EventTSWritable( EventTS ets ) {
		
	}
	
	public EventTS get() {
		return null;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		 
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		 
		
	}

}
