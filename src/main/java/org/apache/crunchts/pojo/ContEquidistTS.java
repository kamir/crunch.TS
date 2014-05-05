package org.apache.crunchts.pojo;

import java.util.Iterator;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

public class ContEquidistTS extends AbstractTS {
	
	public ContEquidistTS() { };
	
	static final double default_sr = 1.0;  // 1.0 Hz; one per second

	double sr = 1.0;  					   // default: 1.0 Hz; one per second
	double[] points = null;
	
	public void setData( VectorWritable vector, String label, long t0 ) {
		super.label = label;
		super.tStart = t0;
		setData( vector, default_sr, label );
	}
	
	/**
	 * Conversion of a Vector into an array of doubles.
	 *  
	 * @param VectorWritable vector
	 * @param double customSR
	 */
	public void setData( VectorWritable vector, double customSR, String label ) {
		sr = customSR;		
		points = new double[vector.get().size()];
			
		int c = 0;
	    Iterator<Element> i = vector.get().iterator();
	    while( i.hasNext() ) {
			points[c] = i.next().get(); 
			c++;
		}
	    
	    double dist = sr * 1000.0 * (double)c; // length of ts in s
	    super.tEnd = super.tStart + (long)dist;
	}
	
	/**
	 * Creation of a Vector from an array of doubles.
	 * Allows us to use mathematical operations, which
	 * are implemented in Apache Mahout libraries.
	 *  
	 * @return VectorWritable vector
	 */
	public VectorWritable getData() {
		return null;
	}
}


