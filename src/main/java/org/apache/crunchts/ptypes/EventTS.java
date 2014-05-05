package org.apache.crunchts.ptypes;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Date;
import java.awt.List;
import java.net.URI;

import org.apache.crunch.MapFn;
import org.apache.crunch.types.writable.Writables;
import org.apache.crunchts.pojo.AbstractTS;

import de.bitocean.crunchts.data.EventTSRecord;
import de.bitocean.crunchts.data.Event;

public class EventTS extends AbstractTS {
	
	public EventTS() { };

	Hashtable<Date,Double> values;
	Hashtable<Date,URI> tags;
	
	public void setData( 
			Hashtable<Date,Double> v, 
			Hashtable<Date,URI> t, 
			String label, 
			Date s, 
			Date e ) {
		
		super.label = label;
		values = v;
		tags = t;

	    super.tStart = s.getTime();
	    super.tEnd = e.getTime();
	}
	
	/**
	 * Mapping of an Avro record to a POJO.
	 * 
	 * @param v
	 */
	public void setData( EventTSRecord record ) { 
				
	};

	/** 
	 * If no record is available and testmode is on,
	 * we create a random record on the fly.
	 * 
	 * @return EventTSRecord
	 */
	public EventTSRecord getRecord() {
		if (values == null) return getDefaultRandomRecord( 10 );
		else return createRecord();
	}

	/**
	 * Mapping of POJO to an Avro record, generated from 
	 * Avro schema (eventts.avsc)
	 * 
	 * @return EventTSRecord
	 */
	private EventTSRecord createRecord() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * For tests, we need some random records. So we create those here.
	 * 
	 * @param z : number of events in the random record.
	 * 
	 * @return
	 */
	private EventTSRecord getDefaultRandomRecord(int z) {
		
		ArrayList<Event> eventArray = new ArrayList<Event>();
		for(int i = 0; i < z; i++ ) {
			Event e = new Event( System.currentTimeMillis() , "URI", Math.random() );
			eventArray.add(e);
		}
		
		label = "randomRecord " + System.currentTimeMillis();
		EventTSRecord rec = new EventTSRecord(eventArray, label, tStart, tEnd);
		
		return rec;
	}
	
//	public static org.apache.crunch.types.PType getWritablePType() {
//		 return Writables.derived(
//			      EventTS.class,
//			      new MapFn<EventTSWritable, EventTS>() {
//					private static final long serialVersionUID = 1L;
//					public EventTS map(EventTSWritable etsw) { return etsw.get(); }
//			      },
//			      new MapFn<EventTS, EventTSWritable>() {
//   				private static final long serialVersionUID = 1L;
//			        public EventTSWritable map(EventTS ets) { return new EventTSWritable(ets); }
//			      },
//			      Writables.writables(EventTSWritable.class));		
//   }

}












