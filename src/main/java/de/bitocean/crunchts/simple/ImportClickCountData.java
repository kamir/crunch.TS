package de.bitocean.crunchts.simple;


import java.util.HashSet;
import java.util.Vector;

import org.apache.crunch.impl.mr.run.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.crunch.util.*;
import org.apache.crunch.*;
import org.apache.crunch.types.*;
import org.apache.crunch.Pair;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.io.avro.AvroFileTarget;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableType;
import org.apache.crunch.types.writable.Writables;
import org.apache.crunch.util.CrunchTool;
import org.apache.crunchts.pojo.ContEquidistTS;
import org.apache.crunchts.types.wikipedia.analysis.ClickCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.VectorWritable;
import com.google.common.base.Supplier;
 
public class ImportClickCountData extends CrunchTool {
	
	private static final long serialVersionUID = 1L;

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.printf("Usage: ImportClickCountData <input dir> <output dir>\n");
			System.exit(-1);
		}

		String outpath = args[1] + "_" + System.currentTimeMillis();
		System.out.println("in  : " + args[0] );
		System.out.println("out : " + outpath );
				
		// load the log lines from TextFiles
		PCollection<String> raw = read( 
				From.textFile( args[0] ) 
	    );     
		
		HashSet<String> neighborhood = new HashSet<String>();
		neighborhood.add( "de.DAX" );
		
		PCollection<ClickCount> converted = covertFromString( raw, neighborhood );
		
		AvroFileTarget target = new AvroFileTarget( new Path( outpath ) );
		this.write( converted, target);
		
		PipelineResult result = done();
		
		return result.succeeded() ? 0 : 1;		
	}

	private PCollection<ClickCount> covertFromString(PCollection<String> raw, final HashSet<String> neighborhood ) {
		
		return raw.parallelDo( new DoFn<String, ClickCount>() {
			@Override
		    public void process(String r, Emitter<ClickCount> emitter) {
								
				String[] c = r.split(" ");
				
				if ( neighborhood != null ) {
					if ( c[0].contains( "." )  ) return;
					if ( !neighborhood.contains( c[0] + "." + c[1] ) ) return;
				}
				
				Supplier<InputSplit> supplier = (Supplier<InputSplit>) ((MapContext) getContext()).getInputSplit();
			    InputSplit underlying = (InputSplit)supplier.get();
				
				FileSplit fileSplit = (FileSplit)underlying;
			    String filename = fileSplit.getPath().getName();
			    System.out.println("Directory and File name : "+fileSplit.getPath().toString());
 
				String[] d = filename.split("-");
								
				ClickCount out = new ClickCount();
				
				out.setYear( Integer.parseInt( d[1].substring(0, 3) ) );
				out.setMonth( Integer.parseInt( d[1].substring(4, 5) ) );
				out.setDay( Integer.parseInt( d[1].substring(6, 7) ) );
				out.setHour( d[2] );
				
				out.setProjectname( c[0] );
				out.setPagename( c[1] );
				out.setClicks( Long.parseLong( c[2] ) );
				out.setVolume( Long.parseLong( c[3] ) );
								
				emitter.emit( out );
			}
		},
		Avros.reflects( ClickCount.class ) );			
	}

 

	public PCollection<Double> countAllClicks(PTable<Text,VectorWritable> ts) {
		return ts.parallelDo("calc total number of clicks", new SimpleClickCountFn(), Avros.doubles() );
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new ImportClickCountData(), args);
		System.exit(exitCode);
	}
}
