package de.bitocean.crunchts.sessionization;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import com.loudacre.data.WebHit;

/**
 * After sessionization, finds the most helpful or last document an account looked at before stopping their session
 */
public class MostHelpfulFn extends DoFn<Pair<String, Iterable<Pair<Long, WebHit>>>, String> {
	private static final long serialVersionUID = 1L;

	/** The maximum time in milliseconds for a session to last */
	long sessionTime;

	/**
	 * Constructor
	 * 
	 * @param sessionTime
	 *            The maximum time in milliseconds for a session to last
	 */
	public MostHelpfulFn(long sessionTime) {
		this.sessionTime = sessionTime;
	}

	/**
	 * Process the WebHit objects to the most helpful or last document an account looked at before stopping their
	 * session
	 * 
	 * @param hits
	 *            The list of all WebHits for an account in order by timestamp. The objects in order from left to right:<br>
	 *            String accountid - The account id from the log line<br>
	 *            Long timestamp - The timestamp from the log line for the action<br>
	 *            WebHit webHit - The WebHit object instantiated with all of the information
	 * @param emitter
	 *            The emitter to output the most helpful or last document an account looked at before stopping their
	 *            session
	 */
	@Override
	public void process(Pair<String, Iterable<Pair<Long, WebHit>>> hits, Emitter<String> emitter) {
		WebHit previousHit = null;

		for (Pair<Long, WebHit> timeAndHit : hits.second()) {
			WebHit currentHit = timeAndHit.second();

			if (previousHit == null) {
				// First time through the loop, set the variables up
				previousHit = WebHit.newBuilder(currentHit).build();
			} else {
				// Calculate time between hits
				long elapsed = timeAndHit.second().getHitTimestamp() - previousHit.getHitTimestamp();

				if (elapsed > sessionTime) {
					// Too much time has passed, create a new session
					emitter.emit(previousHit.getPageHit().toString());
				}

				previousHit = WebHit.newBuilder(currentHit).build();
			}
		}

		// Output the last session
		if (previousHit != null) {
			emitter.emit(previousHit.getPageHit().toString());
		}
	}
}
