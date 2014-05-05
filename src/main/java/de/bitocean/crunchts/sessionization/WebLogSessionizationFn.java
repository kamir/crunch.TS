package de.bitocean.crunchts.sessionization;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.log4j.Logger;

import com.loudacre.data.WebHit;

/**
 * Takes the log lines and transforms them in to WebHit objects.
 */
public class WebLogSessionizationFn extends DoFn<String, Pair<String, Pair<Long, WebHit>>> {
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(WebLogSessionizationFn.class);

	/** Regex to find all Knowledge Base articles */
	Pattern articlePattern = Pattern.compile("KBDOC-\\d+\\.html");

	/** Regex to find dates in the log */
	Pattern dateAndAccountIdPattern = Pattern
			.compile("-\\s(\\d*)\\s\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}) .*\\]");

	/** Date pattern in the log */
	SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");

	/**
	 * Process the incoming log lines and create the WebHit objects
	 * 
	 * @param line
	 *            The incoming log line to process
	 * @param emitter
	 *            The emitter to output the parsed objects. The objects should be in order from left to right:<br>
	 * String accountid - The account id from the log line<br>
	 * Long timestamp - The timestamp from the log line for the action<br>
	 * WebHit webHit - The WebHit object instantiated with all of the information
	 */
	@Override
	public void process(String line, Emitter<Pair<String, Pair<Long, WebHit>>> emitter) {
		// Create the Matcher object to see if the line passes the Regex
		Matcher articleMatcher = articlePattern.matcher(line);

		if (articleMatcher.find()) {
			// Pull out the Knowledge Base article name and emit
			String knowledgeBaseArticle = articleMatcher.group();

			Matcher dateAndAccountMatcher = dateAndAccountIdPattern.matcher(line);

			// Parse out the date and account id
			if (dateAndAccountMatcher.find()) {
				try {
					String accountId = dateAndAccountMatcher.group(1);
					Date logDate = dateFormat.parse(dateAndAccountMatcher.group(2));

					// Create the objects and emit
					WebHit webHit = new WebHit(accountId, knowledgeBaseArticle, logDate.getTime());

					Pair<Long, WebHit> timeAndHit = new Pair<Long, WebHit>(webHit.getHitTimestamp(), webHit);
					Pair<String, Pair<Long, WebHit>> accountIdAndHit = new Pair<String, Pair<Long, WebHit>>(accountId,
							timeAndHit);
					emitter.emit(accountIdAndHit);
				} catch (ParseException e) {
					logger.error(e);
				}
			}
		}
	}
}
