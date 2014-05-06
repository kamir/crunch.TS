package org.apache.crunchts.simple;


import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.io.From;
import org.apache.crunch.io.avro.AvroFileTarget;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.util.CrunchTool;
import org.apache.crunchts.pojo.EventTS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.VectorWritable;
import org.apache.crunchts.pojo.ContEquidistTS;
import org.apache.crunchts.types.EventTSRecord;
 
/**
 * Crunch time series processing pipeline.
 * 
 * Step 1: Covert data from SequenceFile representation into Avro records.
 * 
 * Step 2: Extract event time series from continuous equidistant time series. 
 * 
 * @author Mirko K'mpf
 *
 */
public class ExtractEventTimeSeriesFromTSBucket extends CrunchTool {
	
	private static final long serialVersionUID = 1L;

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.printf("Usage: ExtractEventTimeSeriesFromTSBucket <input file> <output dir>\n");
			System.exit(-1);
		}

		// load the time series from SequenceFiles
		PTable<Text,org.apache.mahout.math.VectorWritable> tsb = read( 
				From.sequenceFile(  
						args[0],                                            // location in HDFS 
						Text.class,                                         // key-type
						org.apache.mahout.math.VectorWritable.class ) );    // value-type
		
		PCollection<ContEquidistTS> converted = covertFromVectorWritables( tsb );
		
		PCollection<EventTSRecord> extracted = extractContEquidistTS( converted );
			
		AvroFileTarget target1 = new AvroFileTarget( new Path( args[1] + "_converted_ced_avro" ) );
		this.write( converted, target1);
		
		AvroFileTarget target2 = new AvroFileTarget( new Path( args[1] + "_extracted_events_avro" ) );
		this.write( extracted, target2);
		
		PipelineResult result = done();
		
		return result.succeeded() ? 0 : 1;		
	}

	private PCollection<EventTSRecord> extractContEquidistTS(
			PCollection<ContEquidistTS> tsb) {

		return tsb.parallelDo( new DoFn<ContEquidistTS,EventTSRecord>() {
			@Override
		    public void process( ContEquidistTS ts, Emitter<EventTSRecord> emitter) {
				
					VectorWritable v = ts.getData();
					EventTS out = new EventTS();
					
					emitter.emit( out.getRecord() );
				}
			},		
			Avros.records(EventTSRecord.class) 
		);
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
		int exitCode = ToolRunner.run(new Configuration(), new ExtractEventTimeSeriesFromTSBucket(), args);
		System.exit(exitCode);
	}
}
