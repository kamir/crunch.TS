package de.bitocean.crunchts.simple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DecimalFormat;
import java.util.Hashtable;
import java.util.Iterator;
import org.apache.crunchts.types.EventTSRecord;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.io.From;
import org.apache.crunch.io.avro.AvroFileTarget;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.util.CrunchTool;
import org.apache.crunchts.pojo.ContEquidistTS;
import org.apache.crunchts.ptypes.EventTS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.VectorWritable;

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
public class CalcTSBProfile extends CrunchTool {

    private static final long serialVersionUID = 1L;

    @Override
    public int run(String[] args) throws Exception {

        /**
         *
         * 1 CN: 1.0 1 IWL: 19.0 1 A.L: 171.0 1 B.L: 566.0
         *
         * 2 CN: 1.0 2 IWL: 91.0 2 A.L: 1128.0 2 B.L: 17293.0
         *
         * 3 CN: 1.0 3 IWL: 103.0 3 A.L: 551.0 3 B.L: 7270.0
         *
         * 4 CN: 1.0 4 IWL: 107.0 4 A.L: 684.0 4 B.L: 7781.0
         *
         *
         * 
         * 
         * 
         * THIS DATA was loaded via: 
         * 
         * project: WikiExplorer
         * package: wikipedia.corpus.extractor.MetadataInspector
         * 
         * 
         */
        Hashtable<String, Integer> pages = new Hashtable<String, Integer>();

        pages.put("1.CN", 1);
        pages.put("1.IWL", 19);
        pages.put("1.A.L", 171);
        pages.put("1.B.L", 566);

        pages.put("2.CN", 1);
        pages.put("2.IWL", 91);
        pages.put("2.A.L", 1128);
        pages.put("2.B.L", 17293);

        pages.put("3.CN", 1);
        pages.put("3.IWL", 103);
        pages.put("3.A.L", 551);
        pages.put("3.B.L", 7270);

        pages.put("4.CN", 1);
        pages.put("4.IWL", 107);
        pages.put("4.A.L", 684);
        pages.put("4.B.L", 7781);

        String folder = "/Volumes/MyExternalDrive/SHARE.VM.MAC/DATA/by_CN_AND_Group";
        String studie = "dissertation_DEMO";

        BufferedWriter wr = new BufferedWriter(new FileWriter(folder + "/profile.dat"));

        wr.write("group & nrOfPages & zAccessTS & sum(A) & <a> & zEditTS & sum(E) & <E> & ");

        String[] groups = {"2", "4"};

        for (String g : groups) {

            Profile pro1 = new Profile( pages, studie, g, "CN", folder);
            Profile pro2 = new Profile( pages, studie, g, "IWL", folder);
            Profile pro3 = new Profile( pages, studie, g, "A.L", folder);
            Profile pro4 = new Profile( pages, studie, g, "B.L", folder);

            pro1 = calcProfile(pro1, true);
            pro2 = calcProfile(pro2, true);
            pro3 = calcProfile(pro3, true);
            pro4 = calcProfile(pro4, true);

            wr.write( pro1.line.toString() + "\t");
            wr.write( pro2.line.toString() + "\t");
            wr.write( pro3.line.toString() + "\t");
            wr.write( pro4.line.toString() + "\t");

        }

        String[] groups2 = {"1"};

        for (String g : groups2) {

            Profile pro1 = new Profile( pages, studie, g, "CN", folder);
            Profile pro2 = new Profile( pages, studie, g, "IWL", folder);
            Profile pro3 = new Profile( pages, studie, g, "A.L", folder);
            Profile pro4 = new Profile( pages, studie, g, "B.L", folder);

            pro1 = calcProfile(pro1, false);
            pro2 = calcProfile(pro2, false);
            pro3 = calcProfile(pro3, false);
            pro4 = calcProfile(pro4, false);

            wr.write(pro1.line.toString() + "\t");
            wr.write(pro2.line.toString() + "\t");
            wr.write(pro3.line.toString() + "\t");
            wr.write(pro4.line.toString() + "\t");

        }
        wr.flush();
        wr.close();

        return 0;
    }

    public Profile calcProfile(Profile pro, boolean full) {

        DecimalFormat df1 = new DecimalFormat("0");
        DecimalFormat df2 = new DecimalFormat("0.00");
        DecimalFormat df3 = new DecimalFormat("0");
        DecimalFormat dfProzent = new DecimalFormat("0 $\\%$");

        String group = pro.group;

        String[] args = pro.args;

        // load the time series from SequenceFiles
        PTable<Text, org.apache.mahout.math.VectorWritable> tsbA = read(
                From.sequenceFile(
                        args[0], // location in HDFS 
                        Text.class, // key-type
                        org.apache.mahout.math.VectorWritable.class));    // value-type

        PCollection<Double> countsA = countAllClicks(tsbA);
        long nrOfSeriesA = tsbA.length().getValue();

        Iterator<Double> itA = ((org.apache.crunch.materialize.MaterializableIterable) countsA.materialize()).iterator();

        double sumA = 0.0;
        while (itA.hasNext()) {
            sumA = sumA + itA.next();
        }

        double AactivityPerDayAndPage = sumA / (1.0 * nrOfSeriesA * 365);
        pro.append("nrOfSeriesA", nrOfSeriesA, df1);
//        pro.append("sumA", sumA, df1);
        pro.append("AactivityPerDayAndPage", AactivityPerDayAndPage, df3);
        pro.append("coverage", 100.0 * (double)nrOfSeriesA / (double)pro.nrPages, df2);

        if (full) {

            // load the time series from SequenceFiles
            PTable<Text, org.apache.mahout.math.VectorWritable> tsbE = read(
                    From.sequenceFile(
                            args[1], // location in HDFS 
                            Text.class, // key-type
                            org.apache.mahout.math.VectorWritable.class));    // value-type

            long nrOfSeriesE = tsbE.length().getValue();

            PCollection<Double> countsE = countAllClicks(tsbE);

            Iterator<Double> itE = ((org.apache.crunch.materialize.MaterializableIterable) countsE.materialize()).iterator();

            double sumE = 0.0;
            while (itE.hasNext()) {
                sumE = sumE + itE.next();
            }
            double EactivityPerDayAndPage = sumE / (1.0 * nrOfSeriesE);

            pro.append("nrOfSeriesE", nrOfSeriesE, df1);
//            pro.append("sumE", sumE, df1);
            pro.append("EactivityPerDayAndPage", EactivityPerDayAndPage, df3);
            pro.append("coverage", 100.0 * (double)nrOfSeriesE / (double)pro.nrPages, df2);

        }
        
        PipelineResult result = done();

        int i = result.succeeded() ? 0 : 1;

//        pro.line.append(" & status:" + i);
        return pro;
    }

    private PCollection<EventTSRecord> extractContEquidistTS(
            PCollection<ContEquidistTS> tsb) {

        return tsb.parallelDo(new DoFn<ContEquidistTS, EventTSRecord>() {
            @Override
            public void process(ContEquidistTS ts, Emitter<EventTSRecord> emitter) {

                VectorWritable v = ts.getData();
                EventTS out = new EventTS();

                emitter.emit(out.getRecord());
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

        return tsb.parallelDo(new DoFn<Pair<Text, VectorWritable>, ContEquidistTS>() {
            @Override
            public void process(Pair<Text, VectorWritable> ts, Emitter<ContEquidistTS> emitter) {

                String key = ts.first().toString();
                VectorWritable v = ts.second();

                ContEquidistTS out = new ContEquidistTS();
                out.setData(v, key, 0);

                emitter.emit(out);
            }
        },
                Avros.reflects(ContEquidistTS.class));

    }

    public PCollection<Double> countAllClicks(PTable<Text, VectorWritable> ts) {
        return ts.parallelDo("calc total number of clicks", new SimpleClickCountFn(), Avros.doubles());
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new CalcTSBProfile(), args);
        System.exit(exitCode);
    }
}

class Profile {

    int nrPages = 0;
    
    public Profile(Hashtable<String,Integer> pages, String studie, String g, String label, String path) {
        group = studie + "_" + g + "_" + label;
        String g2 = group.replace("_", " ");
        nrPages = pages.get( g + "." + label );
        line.append("\\\\\n" + g + " " + label + " & " + nrPages );
        args[0] = path + "/" + group + ".accessrate.tsb.seq";
        args[1] = path + "/" + group + ".editrate.tsb.seq";
    }

    String group = null;
    StringBuffer line = new StringBuffer();

    String[] args = new String[2];

    long nrOfSeries = 0;
    long sum = 0;

    public void append(String label, double v, DecimalFormat df) {
        line.append(" & " + df.format(v));
    }

}
