package org.apache.crunch.vectors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * The properties Vector uses, e.g. a "JSON-String to carry some Metadata with it".
 * A different wrapper can handle triples, which are stored in an RDF store
 * to describe the "data lineage graph".
 * 
 * In opposite to the DOAD file (description of a data set) which is a macroscopic 
 * description of the whole data set, this DOAR element is the microscopic description 
 * of its elements.
 * 
 * Example: We have measured time series data, like wind speed in several locations.
 * 
 * the measured data is the a key value pair: (k,v) = (time,speed) 
 * the units of measurement is already part of the metadata: [s since t0, km/h]  
 * but in order to define relations between measurements, more data is required.
 * 
 * Implicit information allows to calculate the average value or ordering in time.
 * 
 * But the measurement does not contain location data, nor the device identifier or
 * even device parameters, which might be required to validate the obtained values.
 * 
 * Therefore we use DOAD and DOAR elements, which are compliments to existing.
 *  
 *    DOAD            DOAR           DOAP             PMML
 *    
 *    Description     Description    Description      Predictive Model Markup Language 
 *    of a dataset    of a record    of a project
 * 
 * DOAP and PMML are well known for many years and adopted in many groups. Both are highly correlated
 * hot IT projects worked during the last years. More and more tools have been developed since
 * the Internet allows agile and flexible development processes. The Linux operating system, the 
 * whole Eclipse platform and now the Hadoop ecosystem show in three stages how highly complex systems
 * can emerge from simple tools - this structure formation process requires more than just developers
 * which contribute source code and documentation. Nobody can read all produced texts. Automatic consistency
 * like the dependency checks and type matching analysis which are done during an automatic build cycle are 
 * essential incredients herefore. 
 * 
 * A DOAP file is the description of a project and contains relevant information
 * which allow users to identify if a software project fits or not quickly. The use programming language, supported
 * operation systems, the scope and even the address of the list of known bugs are included. A scan of this this 
 * information, manually or even via tools speeds up the communication process and lowers the barriers for
 * large scale colaboration. 
 * 
 * The results of data analysis procedures are either data tables or charts. In many cases it is not
 * sufficient just to show a plot. The relation between variables is described by a formula which contains
 * a high or low number less constants, derived from the data set. Such models can than be reused. To share
 * such information the PMML was developed since 1997. The contributors have formed the data mining group 
 * DMG (http://www.dmg.org/). Especially theresults of data analysis procedures like classifiers or decision trees
 * are in the focus or the PMML, but not the data set for itselfe.
 * 
 * One has to track the location of all measurement devices and their configuration parameters together with the data set 
 * to achieve traceability. The emerging semantic web offers a technique called RDF graphs, to connect pieces of
 * information with each other. The "Internet of things" already uses this approach. All static metadata about a device 
 * can be stored in database which offers its content using a triple store, which supports so called SPARQL queries. 
 * This allows to point to this details via one single URI. All status information are stored in this system and can be
 * requested via a SPARGL query, starting with the URI of one device. What does this mean for our data set?
 * Not the full set of metadata is required in each data set, one URI us all what is needed a connecting link 
 * to the metadata layer.
 * 
 * Such explicit metadata should not be used very often during data processing. To find out, what the brand and type of
 * a measurement device was it is good to use the external layer, but for correlation analysis such explicit metadata
 * objects have to be aggregated and included into the data set. E.g. the list of geographical locations of all 
 * weather from which one has data points or the size of age and gender od person, participating in a social communication network.
 * One has to be careful and should not be confused by explicit and implicit metadata. Both types can be converted into
 * each other with data aggregation and ectraction procedures which finally form a well defined data set, for which relevant
 * connections to explicit datasets (in this case metadata) are known. All this information is part of the DOAP file, which 
 * allows automatic lookups of background information to datasets.
 * 
 * In many cases, the record metadata is not required in each step of an analysis algorithm. E.g. the k-means classification
 * can be calculated without background knowledge about the data vector which is provided to the algorithm. Each vector represents one
 * object and has an id, this is enough but later during processing, but at the end one has to assign a value which represents the
 * class to that id and this class can become part of the metadata about the object and therefore it should be stored in the
 * metastore as well. In this case the data set contains a URI per vector representing an object. URIs are inefficient because of the
 * higher memory footprint. An internal dictionary is used to track the vector labels and their external URI. As soon as a result
 * dataset is stored, the new information can become an own entitiy in the semantic graph. For each record we add a "classifier result" 
 * which consists of the obtained class or classes and a link to the metainformation about this analysis step. What classification algorithm,
 * what implementation and what parameters have been used to produce this results are part of the analysis step metadata.
 * 
 * Such an integrated approach allows even incremental analysis because all relevant parameters are conserved and can be obtained from the
 * semantic meta stores automatically via appropriate queries, which replace the traditional manually parameter tracking procedures.
 * 
 * @author Mirko Kaempf
 *
 */
public class PropertiesVector extends NamedVector {

	public static String kvpSEP = ":";
	public static String kvSEP = "#";
	
	transient IPropHandler propHandler = new StringPropHandler();
	byte[] props = null;
	
	public PropertiesVector( NamedVector nv, String key, String val ) {
		super( nv, key + kvSEP + val );
		String toc="toc" + kvSEP + System.currentTimeMillis();
		if( props == null ) props = toc.getBytes();
		addProperty( key,val );
	}
	
	public void addProperty(Object key,	Object value) {
		String kvp = key + kvSEP + value;
		try {
			byte[] p = propHandler.mergeProperties(kvp, props);
			props = p;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String toString() {
		String name = "METADATA : " + getName();
		return name.concat( "\nRECORD   : " + getDelegate().toString() );
	}
	
	@Override
	public String getName() {
		return (String) propHandler.expandProperties(props);
	}

    /**
     * For demo data generation a set of random numbers is multiplied by a factor
     * 
     * @param randomData, factor
     * 
     * @return rescaled data 
     */
    private static double[] rescaleRandomData(double[] randomData, double factor) {
        double sum = 0.0;
        for( int i = 0; i < randomData.length; i++ ) { 
            randomData[i] = randomData[i] * factor;
            sum = sum + randomData[i];
        }
        System.out.println("sum:" + sum + "\tavg:" + sum/(double)randomData.length );
        return randomData;        
    }
    
    private static double[] getRandomData( int z) {
        double[] d = new double[z];
        for(int i = 0 ; i < z; i++ ) {
            d[i] = Math.random();
        }
        return d;
    }
	
	public static void main(String[] args) {

		// simulate a measurement of one time series 
		double[] data = rescaleRandomData( getRandomData((int) Math.pow(2, 3)) , 24.0 );
        NamedVector nv = new NamedVector(new DenseVector(data), "random data");
        
        System.out.println( "> Initial data: " + nv.toString() );
        System.out.println( );
        
        PropertiesVector pv = new PropertiesVector( nv , "name", nv.getName() );

        pv.addProperty("Hey", "You!");
        pv.addProperty("I like", "Vectors");

        System.out.println( pv.toString() );
        
	}
	
}
