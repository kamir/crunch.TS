package org.apache.crunchts.simple;


import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.io.From;
import org.apache.crunch.io.avro.AvroFileTarget;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.util.CrunchTool;
import org.apache.crunchts.pojo.ContEquidistTS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.VectorWritable;
 

public class ConvertTSBucket extends CrunchTool {
	
	private static final long serialVersionUID = 1L;

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.printf("Usage: ConvertTSBucket <input file> <output dir>\n");
			System.exit(-1);
		}

		// load the time series from SequenceFiles
		PTable<Text,org.apache.mahout.math.VectorWritable> tsb = read( 
				From.sequenceFile(  
						args[0],                                            // location in HDFS 
						Text.class,                                         // key-type
						org.apache.mahout.math.VectorWritable.class ) );    // value-type
		
		PCollection<ContEquidistTS> converted = covertFromVectorWritables( tsb );
		
		PObject<Long> zRecords = converted.length();
		
		// Get the counts
		PCollection<Double> hitCounts = countAllClicks( tsb );
		
		/**
		 *  Interesting: here we trigger two different MR-Jobs.
		 */
		PObject<Double> max = hitCounts.max();
		PObject<Double> min = hitCounts.min();
		
		System.out.println( "# of records: " + zRecords.getValue() );
		System.out.println( "min count: " + min.getValue() );
		System.out.println( "max count   : " + max.getValue() );

		AvroFileTarget target = new AvroFileTarget( new Path( args[1] ) );
		this.write( converted, target);
		
		PipelineResult result = done();
		
		return result.succeeded() ? 0 : 1;		
	}

	/**
	 * 
	 * @param tsb
	 * @return
	 */
	private PCollection<ContEquidistTS> covertFromVectorWritables(PTable<Text, VectorWritable> tsb) {
		
		return tsb.parallelDo( new DoFn<Pair<Text, VectorWritable>, ContEquidistTS>() {
			@Override
		    public void process(Pair<Text, VectorWritable> ts, Emitter<ContEquidistTS> emitter) {
				
				String key = ts.first().toString();
				VectorWritable v = ts.second();
				
				ContEquidistTS out = new ContEquidistTS();
				out.setData(v, key, 0);
				
				emitter.emit( out );
			}
		},
		Avros.reflects( ContEquidistTS.class ) );	
		
	}

	public PCollection<Double> countAllClicks(PTable<Text,VectorWritable> ts) {
		return ts.parallelDo("calc total number of clicks", new SimpleClickCountFn(), Avros.doubles() );
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new ConvertTSBucket(), args);
		System.exit(exitCode);
	}
}
