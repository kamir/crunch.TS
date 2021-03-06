package de.bitocean.crunchts.simple;

import java.util.HashSet;

import org.apache.crunch.impl.mr.run.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.crunch.util.*;
import org.apache.crunch.*;
import org.apache.crunch.Pair;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.io.From;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.util.CrunchTool;
import org.apache.crunchts.types.wikipedia.analysis.ClickCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.VectorWritable;
import com.google.common.base.Supplier;
import org.apache.crunch.io.text.TextFileTarget;
import org.apache.crunch.lib.Shard;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * This example program imports Wikipedia clickCount data into HDFS.
 * We use the Avro representation and also extract year, month and day fields
 * as individual attributes to enable different partition strategies. 
 * 
 * @author kamir
 */
public class ImportClickCountData extends CrunchTool {

    private static final long serialVersionUID = 1L;
    

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println(">>> Import the Wikipedia ClickCount data into HADOOP using the AVRO format.");
            System.out.printf("> Usage: ImportClickCountData <input dir> <output dir>\n");
            System.exit(-1);
        }

        // Validate the parameters ...
        String outpath = args[1] + "_" + System.currentTimeMillis();
        System.out.println("in  : " + args[0]);
        System.out.println("out : " + outpath);

        // Data is a gzip compressed textfile ...
        Source<String> mySource = From.textFile(args[0]);
        mySource.inputConf(RuntimeParameters.DISABLE_COMBINE_FILE, "true");

        // load the log lines from TextFiles
        PCollection<String> raw = read(mySource);
        
        // in case we use FileSplit in a map only job it will still 
        // create a lot of small files once the job is done. 
        // To mitigate this we can do an "identity reduce" using the code below. 
        //
        // We can manually set the number of files 
        // but getRecommendedPartitions has some nice functionality 
        // that can be tuned with crunch.bytes.per.reduce.task(default 1GB) 
        // and crunch.max.reducers(default 500).
        Shard.shard(raw, PartitionUtils.getRecommendedPartitions(raw));

        // What data has to be extracted?
        // Everything or just the pages listed in this neighborhood definition
        HashSet<String> neighborhood = new HashSet<String>();
        neighborhood.add("de_DAX");
        neighborhood.add("de_Stollberg");
        neighborhood.add("de_Berlin");
        neighborhood.add("de_Meiningen");

        // if neighborhood is not null, all records are processed ...
        PCollection<ClickCount> converted = covertFromString(raw, neighborhood);

        // Make a key value pair from all records ...
        PTable<String, ClickCount> kv = extractPageNameAsKey(converted);

        // Group by pagename
        PGroupedTable<String, ClickCount> grouped = kv.groupByKey();

        // AvroFileTarget target = new AvroFileTarget( new Path( outpath ) );
        TextFileTarget target = new TextFileTarget(new Path(outpath));

        write(grouped, target);

        PipelineResult result = done();

        return result.succeeded() ? 0 : 1;
    }

    private PCollection<ClickCount> covertFromString(PCollection<String> raw, final HashSet<String> neighborhood) {

        return raw.parallelDo(new DoFn<String, ClickCount>() {

            String fileName = null;
            long timestamp = 0;

            @Override
            public void initialize() {
                super.initialize();
                try {
                    final Supplier<InputSplit> inputSplitSupplier = (Supplier<InputSplit>) ((MapContext) getContext()).getInputSplit();
                    final InputSplit inputSplit = inputSplitSupplier.get();

                    if (inputSplit instanceof FileSplit) {
                        fileName = ((FileSplit) inputSplit).getPath().getName();
                    } else if (inputSplit instanceof CombineFileSplit) {
                        // Add new file resolution logic here.
                        fileName = ((CombineFileSplit) inputSplit).getPaths()[0].getName();
                    }
                   
                    timestamp = de.bitocean.util.wikipedia.TimeStampTool.getTimeInMillis(fileName);
                } 
                catch (ClassCastException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Could not get file name from input splits", e);
                }
            }

            @Override
            public void process(String r, Emitter<ClickCount> emitter) {

                String[] c = r.split(" ");

                // only if neighborhood is defined we filter ...
                if (neighborhood != null) {
                    // if the projects contains "." it is not a Wikipedia 
                    // project, so we reject it
                    if (c[0].contains(".")) {
                        return;
                    }
                    // if the current pagename project pair is not listed
                    // we do nothing
                    if (!neighborhood.contains(c[0] + "_" + c[1])) {
                        return;
                    }
                }

                ClickCount out = new ClickCount();
                // extract the year month and key field
                String[] d = fileName.split("-");

                out.setYear(Integer.parseInt(d[1].substring(0, 4)));
                out.setMonth(Integer.parseInt(d[1].substring(4, 6)));
                out.setDay(Integer.parseInt(d[1].substring(6, 8)));
                out.setHour(d[2]);

                // timestamp comes from the one input file 
                out.setTimestamp(timestamp);

                // process the input of the file ...
                out.setProjectname(c[0]);
                out.setPagename(c[1]);
                out.setClicks(Long.parseLong(c[2]));
                out.setVolume(Long.parseLong(c[3]));

                emitter.emit(out);
            }
        },
        Avros.reflects(ClickCount.class));
    }

    public PCollection<Double> countAllClicks(PTable<Text, VectorWritable> ts) {
        return ts.parallelDo("calc total number of clicks", new SimpleClickCountFn(), Avros.doubles());
    }

    private PTable<String, ClickCount> extractPageNameAsKey(PCollection<ClickCount> converted) {

        return converted.parallelDo(
                new DoFn<ClickCount, Pair<String, ClickCount>>() {

                    @Override
                    public void process(ClickCount r, Emitter<Pair<String, ClickCount>> emitter) {
                        emitter.emit(new Pair(r.projectname + "_" + r.pagename, r));
                    }
                },
                Avros.tableOf(Avros.strings(), Avros.reflects(ClickCount.class)));
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new ImportClickCountData(), args);
        System.exit(exitCode);
    }


}
