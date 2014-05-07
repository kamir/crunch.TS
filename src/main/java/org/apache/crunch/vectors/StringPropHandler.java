package org.apache.crunch.vectors;

import java.io.UnsupportedEncodingException;

public class StringPropHandler implements IPropHandler {

	@Override
	public Object expandProperties( byte[] props ) {
		String s = "*";
		try {
			s = new String( props , "UTF8" );
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return s;
	}

	@Override
	public  byte[] mergeProperties(Object prop, byte[] v) throws UnsupportedEncodingException {
		String so = new String( v , "UTF8" );
		String sn = (String)prop;
		String s = so.concat( PropertiesVector.kvpSEP ).concat(sn);		
		return s.getBytes("UTF8");
	}

}
