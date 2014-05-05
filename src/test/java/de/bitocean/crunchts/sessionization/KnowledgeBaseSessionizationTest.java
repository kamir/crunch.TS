package de.bitocean.crunchts.sessionization;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.loudacre.data.WebHit;

import de.bitocean.crunchts.sessionization.MostHelpfulFn;
import de.bitocean.crunchts.sessionization.WebLogSessionizationFn;

public class KnowledgeBaseSessionizationTest {
	@Test
	public void testWebLogSessionizationFn() throws ParseException {
		String accountId = "8";
		String timestampStr = "15/Sep/2013:23:59:50";
		String pageHit = "KBDOC-00001.html";

		PCollection<String> inputStrings = MemPipeline.collectionOf("165.32.101.206 - " + accountId + " ["
				+ timestampStr + " +0100] \"GET " + pageHit
				+ " HTTP/1.0\" 200 17446 \"http://www.loudacre.com\"  \"Loudacre CSR Browser\"");
		PTable<String, Pair<Long, WebHit>> fileTypeStrings = inputStrings.parallelDo(new WebLogSessionizationFn(),
				Avros.tableOf(Avros.strings(), Avros.pairs(Avros.longs(), Avros.specifics(WebHit.class))));

		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
		long timestamp = dateFormat.parse(timestampStr).getTime();
		WebHit webHit = new WebHit(accountId, pageHit, timestamp);

		Pair<String, Pair<Long, WebHit>> expectedOutput = new Pair<String, Pair<Long, WebHit>>(accountId,
				new Pair<Long, WebHit>(timestamp, webHit));

		assertEquals("Hit did not match", ImmutableList.of(expectedOutput),
				Lists.newArrayList(fileTypeStrings.materialize()));
	}

	@Test
	public void testMostHelpfulFn() throws ParseException {
		String accountId = "8";
		long sessionTime = 100;

		ArrayList<Pair<Long, WebHit>> inputWebHits = new ArrayList<Pair<Long, WebHit>>();
		inputWebHits.add(createWebHitPair(accountId, 1, "KBDOC-00001.html"));
		inputWebHits.add(createWebHitPair(accountId, 2, "KBDOC-00002.html"));
		inputWebHits.add(createWebHitPair(accountId, 3, "KBDOC-00003.html"));

		Pair<String, Iterable<Pair<Long, WebHit>>> input = new Pair<String, Iterable<Pair<Long, WebHit>>>(accountId,
				inputWebHits);

		@SuppressWarnings("unchecked")
		PCollection<Pair<String, Iterable<Pair<Long, WebHit>>>> inputStrings = MemPipeline.collectionOf(input);
		PCollection<String> fileTypeStrings = inputStrings.parallelDo(new MostHelpfulFn(sessionTime), Avros.strings());

		assertEquals("File type did not match", ImmutableList.of("KBDOC-00003.html"),
				Lists.newArrayList(fileTypeStrings.materialize()));
	}

	@Test
	public void testMostHelpfulFnWithMultipleSessions() throws ParseException {
		String accountId = "8";
		long sessionTime = 100;

		ArrayList<Pair<Long, WebHit>> inputWebHits = new ArrayList<Pair<Long, WebHit>>();
		inputWebHits.add(createWebHitPair(accountId, 1, "KBDOC-00001.html"));
		inputWebHits.add(createWebHitPair(accountId, 2, "KBDOC-00002.html"));
		inputWebHits.add(createWebHitPair(accountId, 3, "KBDOC-00003.html"));

		// Add one outside the session time
		inputWebHits.add(createWebHitPair(accountId, sessionTime * 2, "KBDOC-00004.html"));

		Pair<String, Iterable<Pair<Long, WebHit>>> input = new Pair<String, Iterable<Pair<Long, WebHit>>>(accountId,
				inputWebHits);

		@SuppressWarnings("unchecked")
		PCollection<Pair<String, Iterable<Pair<Long, WebHit>>>> inputStrings = MemPipeline.collectionOf(input);
		PCollection<String> fileTypeStrings = inputStrings.parallelDo(new MostHelpfulFn(sessionTime), Avros.strings());

		assertEquals("File type did not match", ImmutableList.of("KBDOC-00003.html", "KBDOC-00004.html"),
				Lists.newArrayList(fileTypeStrings.materialize()));
	}

	private Pair<Long, WebHit> createWebHitPair(String accountId, long timestamp, String pageHit) {
		return new Pair<Long, WebHit>(timestamp, new WebHit(accountId, pageHit, timestamp));
	}
}
