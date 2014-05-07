package org.apache.crunchts.pojo;

import hadoopts.core.TSData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import statphys.ris.experimental.TSPropertyTester;
import data.series.Messreihe;
 

/**
 * 
 * @author training
 *
 */
public class PropertiesVector {

	Vector v = null;
	Properties props = null;
	
	public PropertiesVector() {
		v = new DenseVector();
		props = new Properties();
	}
	
	public void addProperty(Object key,	Object value) {
		props.put(key, value);
	}
	
	public void expandName( NamedVector nv ) {
		try {
			props = new Properties();
			props.load( new ByteArrayInputStream( nv.getName().getBytes("UTF8")) );
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}		
	
	public NamedVector packName() {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(os);
		String label = "unnkown name";
		props.list(ps);
		try {
			label = os.toString("UTF8");
		} 
		catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		NamedVector nv = new NamedVector( v, label );
		return nv;
	}
	
	public static void main(String[] args) {
		TSData data = new TSData();
        data.dataset = rescaleRandomData( data.getRandomData((int) Math.pow(2, EXP)) , 24.0 );
        Messreihe mr = data.getMessreihe();
        if ( SAMPLES < TSPropertyTester.zSAMPLES) TSPropertyTester.addSample( mr );
        SAMPLES++;
        /**
         * 
         * Here we lose the METADATA of each row!!!
         * 
         */
        System.out.print("  (" + i + ")");
        NamedVector nv = new NamedVector(new DenseVector(data.getData()), data.label);
        VectorWritable vec = new VectorWritable();
        vec.set(nv);
	}
	
}
