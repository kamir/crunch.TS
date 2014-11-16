package org.apache.crunchts.pojo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import de.bitocean.data.TSData;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;


 

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
		
		stdlib.StdRandom.initRandomGen(1);
		TSData data = new TSData();
        data.dataset = data.getRandomData((int) Math.pow(2, 10));
  
        NamedVector nv = new NamedVector(new DenseVector(data.getData()), data.label);
        VectorWritable vec = new VectorWritable();
        vec.set(nv);
        
	}
	
}
