/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.crunchts;

/**
 * 
 * This tool transforms a time series bucket (TSB) into a set of time series
 * pairs or even triples.
 * 
 */


import java.io.IOException;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.crunchts.simple.CombineTimeSeriesPairsAndTriplesFromTSBucket;
import org.apache.crunchts.simple.CombineTimeSeriesPairsFromTSBucket;
import org.apache.crunchts.simple.CombineTimeSeriesTriplesFromTSBucket;
import org.apache.crunchts.simple.ConvertTSBucket;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class provides some DFS administrative access.
 */
public class CrunchTSApp extends FsShell {

    /**
     * An abstract class for the execution of a time series command
     */
    abstract private static class TSToolsCommand extends Command {

        final DistributedFileSystem dfs;

        /**
         * Constructor
         */
        public TSToolsCommand(FileSystem fs) {
            super(fs.getConf());
            if (!(fs instanceof DistributedFileSystem)) {
                throw new IllegalArgumentException("FileSystem " + fs.getUri()
                        + " is not a distributed file system");
            }
            this.dfs = (DistributedFileSystem) fs;
        }
    }

    /**
     * Construct a DFSAdmin object.
     */
    public CrunchTSApp() {
        this(null);
    }

    /**
     * Construct a DFSAdmin object.
     */
    public CrunchTSApp(Configuration conf) {
        super(conf);
    }

    /**
     * Gives a report for a time series bucket.
     *
     * @exception IOException if the tsbucket does not exist.
     */
    public void report(String tsbFilePath) throws IOException {
        
        System.out.println("Time-Series-Bucket report (TSBr) is comming soon ...");

        System.out.println("> path: " + tsbFilePath);

        if (fs instanceof DistributedFileSystem) {

            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            FsStatus ds = dfs.getStatus();

            long capacity = ds.getCapacity();
            long used = ds.getUsed();
            long remaining = ds.getRemaining();
            long presentCapacity = used + remaining;

            System.out.println("FS Configured Capacity: " + capacity + " ("
                    + StringUtils.byteDesc(capacity) + ")");
            System.out.println("FS Present Capacity: " + presentCapacity + " ("
                    + StringUtils.byteDesc(presentCapacity) + ")");
            System.out.println("DFS Remaining: " + remaining + " ("
                    + StringUtils.byteDesc(remaining) + ")");
            System.out.println("DFS Used: " + used + " ("
                    + StringUtils.byteDesc(used) + ")");
            System.out
                    .println("DFS Used%: "
                            + StringUtils
                            .limitDecimalTo2(((1.0 * used) / presentCapacity) * 100)
                            + "%");
        }
    }

    private void printHelp(String cmd) {

        String summary = "hadoop tsb is the command to execute TSB administrative commands.\n"
                + "The full syntax is: \n\n"
                + "hadoop tsb [-report] "
                + "\t[-explode [pairs|triples] ]\n"
                + "\t[-cc]\n"
                + "\t[-es]\n"
                + "\t[-gc]\n" + "\t[-help [cmd]]\n";

        String report = "-report: \tReports basic filesystem information and statistics.\n";

        String explode = "-explode :  Explode the time series bucket into a TS-Pair-Bucket.\n";

        String cc = "-cc:\t";

        String es = "-es:\t";

        String gc = "-gc:\t";

        String help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n"
                + "\t\tis specified.\n";

        if ("report".equals(cmd)) {
            System.out.println(report);
        } else if ("explode".equals(cmd)) {
            System.out.println(explode);
        } else if ("cc".equals(cmd)) {
            System.out.println(cc);
        } else if ("es".equals(cmd)) {
            System.out.println(es);
        } else if ("help".equals(cmd)) {
            System.out.println(help);
        } else if ("gc".equals(cmd)) {
            System.out.println(gc);
        } else {
            System.out.println(summary);
            System.out.println(report);
            System.out.println(explode);
            System.out.println(cc);
            System.out.println(es);
            System.out.println(gc);
            System.out.println(help);
            System.out.println();
            ToolRunner.printGenericCommandUsage(System.out);
        }

    }

    /**
     * @param argv The parameters passed to this program.
     * @exception Exception if the filesystem does not exist.
     * @return 0 on success, non zero on error.
     */
    @Override
    public int run(String[] argv) throws Exception {

        if (argv.length < 1) {
            printHelp("");
            return -1;
        }

        int exitCode = -1;
        int i = 0;
        String cmd = argv[i++];

        //
        // verify that we have enough command line parameters
        //
        if ("-explode".equals(cmd)) {
            if (argv.length != 3) {
                printHelp(cmd);
                return exitCode;
            }
        } else if ("-cc".equals(cmd)) {
            if (argv.length != 2) {
                printHelp(cmd);
                return exitCode;
            }
        } else if ("-es".equals(cmd)) {
            if (argv.length != 2) {
                printHelp(cmd);
                return exitCode;
            }
        }

        // initialize CrunchTSApp
        try {
            init();
        } catch (RPC.VersionMismatch v) {
            System.err.println("Version Mismatch between client and server"
                    + "... command aborted.");
            return exitCode;
        } catch (IOException e) {
            System.err.println("Bad connection to DFS... command aborted.");
            return exitCode;
        }

        exitCode = 0;
        try {
            if ("-report".equals(cmd)) {
                report(null);
            } else if ("-explode".equals(cmd)) {
                explode(argv);
            } else if ("-help".equals(cmd)) {
                if (i < argv.length) {
                    printHelp(argv[i]);
                } else {
                    printHelp("");
                }
            } else {
                exitCode = -1;
                System.err.println(cmd.substring(1) + ": Unknown command");
                printHelp("");
            }
        } catch (IllegalArgumentException arge) {
            exitCode = -1;
            System.err.println(cmd.substring(1) + ": "
                    + arge.getLocalizedMessage());
            printHelp(cmd);
        } catch (RemoteException e) {
            //
            // This is a error returned by hadoop server. Print
            // out the first line of the error mesage, ignore the stack trace.
            exitCode = -1;
            try {
                String[] content;
                content = e.getLocalizedMessage().split("\n");
                System.err.println(cmd.substring(1) + ": " + content[0]);
            } catch (Exception ex) {
                System.err.println(cmd.substring(1) + ": "
                        + ex.getLocalizedMessage());
            }
        } catch (Exception e) {
            exitCode = -1;
            System.err.println(cmd.substring(1) + ": "
                    + e.getLocalizedMessage());
        }
        return exitCode;
    }

    private void explode(String[] argv) {
        if (argv[1].equals("pairs")) {
            pairs(argv);
        } else if (argv[1].equals("triples")) {
            triples(argv);
        } else if (argv[1].equals("pairsandtriples")) {
            pairsAndTriples(argv);
        }
    }

    private void pairsAndTriples(String[] argv) {
        System.out.println("Create TS PAIRS and TRIPLES");
        try {
            String[] arguments = new String[2];
            arguments[0] = argv[1];
            arguments[1] = argv[2];
            int exitCode = ToolRunner.run(new Configuration(), new CombineTimeSeriesPairsAndTriplesFromTSBucket(), arguments);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private void triples(String[] argv) {
        System.out.println("TRIPLES");
        try {
            String[] arguments = new String[2];
            arguments[0] = argv[1];
            arguments[1] = argv[2];
            int exitCode = ToolRunner.run(new Configuration(), new CombineTimeSeriesTriplesFromTSBucket(), arguments);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void pairs(String[] argv) {
        System.out.println("PAIRS");
        try {
            String[] arguments = new String[2];
            arguments[0] = argv[1];
            arguments[1] = argv[2];
            int exitCode = ToolRunner.run(new Configuration(), new CombineTimeSeriesPairsFromTSBucket(), arguments);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Converts TS bucket into new representation.
     *
     * @param argv
     */
    private void convert(String[] argv) {
        System.out.println("TRANSFORM");
        try {
            String[] arguments = new String[2];
            arguments[0] = argv[1];
            arguments[1] = argv[2];
            int exitCode = ToolRunner.run(new Configuration(), new ConvertTSBucket(), arguments);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    protected FileSystem fs;

    protected void init() throws IOException {
        getConf().setQuietMode(true);
        if (this.fs == null) {
            this.fs = FileSystem.get(getConf());
        }
    }

    public static void main(String[] argv) throws Exception {
        int res = ToolRunner.run(new CrunchTSApp(), argv);
        System.exit(res);
    }
}
