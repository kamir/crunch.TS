package org.apache.crunch.vectors;

import java.io.UnsupportedEncodingException;

public interface IPropHandler {
	
	/** 
	 * Properties are always collapsed to a String which allows creation of a bytearray.
	 * 
	 * The Property handler has to expand it to a useful Object.
	 *  
	 * @return
	 */
	Object expandProperties( byte[] props );
	
	/**
	 * A name of a NamedVector can not be changed so we add another container object 
	 * to store the properties, which describe a data series.
	 * 
	 * The vector can than be used to store a time series obtained from one sensor.
	 * Sensor details can be stored in the properties container, as well as trasformation
	 * logs like filters or other time series transformations.
	 * 
	 * @param prop
	 * @return
	 */
	 byte[] mergeProperties( Object prop, byte[] v ) throws UnsupportedEncodingException;
	
}
