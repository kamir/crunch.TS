/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.crunchts;

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

		/** Constructor */
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
	 * Gives a report on how the FileSystem is doing.
	 * 
	 * @exception IOException
	 *                if the filesystem does not exist.
	 */
	public void report() throws IOException {
		System.out.println("TimeSeries Bucket report is omming soon ...");
		if (fs instanceof DistributedFileSystem) {
			DistributedFileSystem dfs = (DistributedFileSystem) fs;
			FsStatus ds = dfs.getStatus();
			long capacity = ds.getCapacity();
			long used = ds.getUsed();
			long remaining = ds.getRemaining();
			long presentCapacity = used + remaining;

			System.out.println("Configured Capacity: " + capacity + " ("
					+ StringUtils.byteDesc(capacity) + ")");
			System.out.println("Present Capacity: " + presentCapacity + " ("
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
		String summary = "hadoop dfsadmin is the command to execute DFS administrative commands.\n"
				+ "The full syntax is: \n\n"
				+ "hadoop dfsadmin [-report] [-safemode <enter | leave | get | wait>]\n"
				+ "\t[-saveNamespace]\n"
				+ "\t[-refreshNodes]\n"
				+ "\t[-refreshServiceAcl]\n" + "\t[-help [cmd]]\n";

		String report = "-report: \tReports basic filesystem information and statistics.\n";

		String safemode = "-safemode <enter|leave|get|wait>:  Safe mode maintenance command.\n"
				+ "\t\tSafe mode is a Namenode state in which it\n"
				+ "\t\t\t1.  does not accept changes to the name space (read-only)\n"
				+ "\t\t\t2.  does not replicate or delete blocks.\n"
				+ "\t\tSafe mode is entered automatically at Namenode startup, and\n"
				+ "\t\tleaves safe mode automatically when the configured minimum\n"
				+ "\t\tpercentage of blocks satisfies the minimum replication\n"
				+ "\t\tcondition.  Safe mode can also be entered manually, but then\n"
				+ "\t\tit can only be turned off manually as well.\n";

		String saveNamespace = "-saveNamespace:\t"
				+ "Save current namespace into storage directories and reset edits log.\n"
				+ "\t\tRequires superuser permissions and safe mode.\n";

		String refreshNodes = "-refreshNodes: \tUpdates the set of hosts allowed "
				+ "to connect to namenode.\n\n"
				+ "\t\tRe-reads the config file to update values defined by \n"
				+ "\t\tdfs.hosts and dfs.host.exclude and reads the \n"
				+ "\t\tentires (hostnames) in those files.\n\n"
				+ "\t\tEach entry not defined in dfs.hosts but in \n"
				+ "\t\tdfs.hosts.exclude is decommissioned. Each entry defined \n"
				+ "\t\tin dfs.hosts and also in dfs.host.exclude is stopped from \n"
				+ "\t\tdecommissioning if it has aleady been marked for decommission.\n"
				+ "\t\tEntires not present in both the lists are decommissioned.\n";

		String finalizeUpgrade = "-finalizeUpgrade: Finalize upgrade of HDFS.\n"
				+ "\t\tDatanodes delete their previous version working directories,\n"
				+ "\t\tfollowed by Namenode doing the same.\n"
				+ "\t\tThis completes the upgrade process.\n";

		String upgradeProgress = "-upgradeProgress <status|details|force>: \n"
				+ "\t\trequest current distributed upgrade status, \n"
				+ "\t\ta detailed status or force the upgrade to proceed.\n";

		String metaSave = "-metasave <filename>: \tSave Namenode's primary data structures\n"
				+ "\t\tto <filename> in the directory specified by hadoop.log.dir property.\n"
				+ "\t\t<filename> will contain one line for each of the following\n"
				+ "\t\t\t1. Datanodes heart beating with Namenode\n"
				+ "\t\t\t2. Blocks waiting to be replicated\n"
				+ "\t\t\t3. Blocks currrently being replicated\n"
				+ "\t\t\t4. Blocks waiting to be deleted\n";

		String refreshServiceAcl = "-refreshServiceAcl: Reload the service-level authorization policy file\n"
				+ "\t\tNamenode will reload the authorization policy file.\n";

		String help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n"
				+ "\t\tis specified.\n";

		if ("report".equals(cmd)) {
			System.out.println(report);
		} else if ("safemode".equals(cmd)) {
			System.out.println(safemode);
		} else if ("saveNamespace".equals(cmd)) {
			System.out.println(saveNamespace);
		} else if ("refreshNodes".equals(cmd)) {
			System.out.println(refreshNodes);
		} else if ("finalizeUpgrade".equals(cmd)) {
			System.out.println(finalizeUpgrade);
		} else if ("upgradeProgress".equals(cmd)) {
			System.out.println(upgradeProgress);
		} else if ("metasave".equals(cmd)) {
			System.out.println(metaSave);
		} else if ("refreshServiceAcl".equals(cmd)) {
			System.out.println(refreshServiceAcl);
		} else if ("help".equals(cmd)) {
			System.out.println(help);
		} else {
			System.out.println(summary);
			System.out.println(report);
			System.out.println(safemode);
			System.out.println(saveNamespace);
			System.out.println(refreshNodes);
			System.out.println(finalizeUpgrade);
			System.out.println(upgradeProgress);
			System.out.println(metaSave);
			System.out.println(refreshServiceAcl);
			System.out.println(help);
			System.out.println();
			ToolRunner.printGenericCommandUsage(System.out);
		}

	}

	/**
	 * Displays format of commands.
	 * 
	 * @param cmd
	 *            The command that is being executed.
	 */
	private static void printUsage(String cmd) {
		if ("-report".equals(cmd)) {
			System.err.println("Usage: crunchts" + " [-report]");
		} else if ("-safemode".equals(cmd)) {
			System.err.println("Usage: crunchts"
					+ " [-safemode enter | leave | get | wait]");
		} else if ("-saveNamespace".equals(cmd)) {
			System.err.println("Usage: crunchts" + " [-saveNamespace]");
		} else if ("-refreshNodes".equals(cmd)) {
			System.err.println("Usage: crunchts" + " [-refreshNodes]");
		} else if ("-finalizeUpgrade".equals(cmd)) {
			System.err.println("Usage: crunchts" + " [-finalizeUpgrade]");
		} else if ("-upgradeProgress".equals(cmd)) {
			System.err.println("Usage: crunchts"
					+ " [-upgradeProgress status | details | force]");
		} else if ("-metasave".equals(cmd)) {
			System.err.println("Usage: crunchts" + " [-metasave filename]");
		} else if ("-refreshServiceAcl".equals(cmd)) {
			System.err.println("Usage: crunchts" + " [-refreshServiceAcl]");
		} else {
			System.err.println("Usage: crunchts");
			System.err.println("           [-report]");
			System.err
					.println("           [-safemode enter | leave | get | wait]");
			System.err.println("           [-saveNamespace]");
			System.err.println("           [-refreshNodes]");
			System.err.println("           [-finalizeUpgrade]");
			System.err
					.println("           [-upgradeProgress status | details | force]");
			System.err.println("           [-metasave filename]");
			System.err.println("           [-refreshServiceAcl]");
			System.err.println("           [-help [cmd]]");
			System.err.println();
			ToolRunner.printGenericCommandUsage(System.err);
		}
	}

	/**
	 * @param argv
	 *            The parameters passed to this program.
	 * @exception Exception
	 *                if the filesystem does not exist.
	 * @return 0 on success, non zero on error.
	 */
	@Override
	public int run(String[] argv) throws Exception {

		if (argv.length < 1) {
			printUsage("");
			return -1;
		}

		int exitCode = -1;
		int i = 0;
		String cmd = argv[i++];

		//
		// verify that we have enough command line parameters
		//
		if ("-convert".equals(cmd)) {
			if (argv.length != 3) {
				printUsage(cmd);
				return exitCode;
			}
		} else if ("-pairs".equals(cmd)) {
			if (argv.length != 3) {
				printUsage(cmd);
				return exitCode;
			}
		} else if ("-triples".equals(cmd)) {
			if (argv.length != 3) {
				printUsage(cmd);
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
				report();
			} else if ("-convert".equals(cmd)) {
				convert(argv);
			} else if ("-pairs".equals(cmd)) {
				pairs(argv);
			} else if ("-triples".equals(cmd)) {
				triples(argv);
			} else if ("-pairsandtriples".equals(cmd)) {
			    pairsAndTriples(argv);
			} else if ("-help".equals(cmd)) {
				if (i < argv.length) {
					printHelp(argv[i]);
				} else {
					printHelp("");
				}
			} else {
				exitCode = -1;
				System.err.println(cmd.substring(1) + ": Unknown command");
				printUsage("");
			}
		} catch (IllegalArgumentException arge) {
			exitCode = -1;
			System.err.println(cmd.substring(1) + ": "
					+ arge.getLocalizedMessage());
			printUsage(cmd);
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

	private void pairsAndTriples(String[] argv) {
		System.out.println("PAIRS and TRIPLES");
		try {
			String[] arguments = new String[2];
			arguments[0] = argv[1];
			arguments[1] = argv[2];
			int exitCode = ToolRunner.run(new Configuration(), new CombineTimeSeriesPairsAndTriplesFromTSBucket(), arguments);
		} 
		catch (Exception e) {
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
		} 
		catch (Exception e) {
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
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void convert(String[] argv) {
		System.out.println("CONVERT");
		try {
			String[] arguments = new String[2];
			arguments[0] = argv[1];
			arguments[1] = argv[2];
			int exitCode = ToolRunner.run(new Configuration(), new ConvertTSBucket(), arguments);
		} 
		catch (Exception e) {
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

	/**
	 * main() has some simple utility methods.
	 * 
	 * @param argv
	 *            Command line parameters.
	 * @exception Exception
	 *                if the filesystem does not exist.
	 */
	public static void main(String[] argv) throws Exception {
		int res = ToolRunner.run(new CrunchTSApp(), argv);
		System.exit(res);
	}
}
