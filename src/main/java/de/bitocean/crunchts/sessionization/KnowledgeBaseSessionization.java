package de.bitocean.crunchts.sessionization;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.loudacre.data.WebHit;

public class KnowledgeBaseSessionization extends CrunchTool {
	private static final long serialVersionUID = 1L;

	public PTable<String, Pair<Long, WebHit>> createHits(PCollection<String> lines) {
		return lines.parallelDo("creating sessions from hits", new WebLogSessionizationFn(),
				Avros.tableOf( Avros.strings(), Avros.pairs(Avros.longs(), Avros.specifics(WebHit.class))));
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: KnowledgeBaseHitCounter <input dir> <output dir>\n");
			System.exit(-1);
		}

		int maximumResults = Integer.parseInt(getConf().get("maximumresults", "20"));

		// Create the list of counts for Knowledge Base articles
		PCollection<String> lines = read(From.textFile(args[0]));
		PTable<String, Pair<Long, WebHit>> hits = createHits(lines);

		// Perform a Secondary Sort to group the accounts and order the timestamps
		PCollection<String> finalHit = SecondarySort.sortAndApply(hits, new MostHelpfulFn(30 * 60 * 1000),
				Avros.strings());
		
		// Get the counts
		PTable<String, Long> finalHitCounts = finalHit.count();

		// Get the top N articles and write them out
		PTable<String, Long> topArticles = finalHitCounts.top(maximumResults);
		topArticles.write(To.textFile(args[1]));

		PipelineResult result = done();
		return result.succeeded() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new KnowledgeBaseSessionization(), args);
		System.exit(exitCode);
	}
}
