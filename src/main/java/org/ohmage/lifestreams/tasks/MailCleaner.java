package org.ohmage.lifestreams.tasks;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ohmage.lifestreams.models.StreamRecord;
import org.ohmage.lifestreams.spouts.EmailMsg;

/**
 * Strip out quote part of a mail. Copy & paste from:
 * http://stackoverflow.com/questions/2385347/how-to-remove-the-quoted-text-from-an-email-and-only-show-the-new-text
 * 
 * @author changun
 *
 */
public class MailCleaner extends SimpleTask<EmailMsg> {
	  /** general spacers for time and date */
	  private static final String spacers = "[\\s,/\\.\\-]";

	  /** matches times */
	  private static final String timePattern  = "(?:[0-2])?[0-9]:[0-5][0-9](?::[0-5][0-9])?(?:(?:\\s)?[AP]M)?";

	  /** matches day of the week */
	  private static final String dayPattern   = "(?:(?:Mon(?:day)?)|(?:Tue(?:sday)?)|(?:Wed(?:nesday)?)|(?:Thu(?:rsday)?)|(?:Fri(?:day)?)|(?:Sat(?:urday)?)|(?:Sun(?:day)?))";

	  /** matches day of the month (number and st, nd, rd, th) */
	  private static final String dayOfMonthPattern = "[0-3]?[0-9]" + spacers + "*(?:(?:th)|(?:st)|(?:nd)|(?:rd))?";

	  /** matches months (numeric and text) */
	  private static final String monthPattern = "(?:(?:Jan(?:uary)?)|(?:Feb(?:uary)?)|(?:Mar(?:ch)?)|(?:Apr(?:il)?)|(?:May)|(?:Jun(?:e)?)|(?:Jul(?:y)?)" +
	                                              "|(?:Aug(?:ust)?)|(?:Sep(?:tember)?)|(?:Oct(?:ober)?)|(?:Nov(?:ember)?)|(?:Dec(?:ember)?)|(?:[0-1]?[0-9]))";

	/** matches years (only 1000's and 2000's, because we are matching emails) */
	  private static final String yearPattern  = "(?:[1-2]?[0-9])[0-9][0-9]";

	  /** matches a full date */
	  private static final String datePattern     = "(?:" + dayPattern + spacers + "+)?(?:(?:" + dayOfMonthPattern + spacers + "+" + monthPattern + ")|" +
	                                                "(?:" + monthPattern + spacers + "+" + dayOfMonthPattern + "))" +
	                                                 spacers + "+" + yearPattern;

	  /** matches a date and time combo (in either order) */
	  private static final String dateTimePattern = "(?:" + datePattern + "[\\s,]*(?:(?:at)|(?:@))?\\s*" + timePattern + ")|" +
	                                                "(?:" + timePattern + "[\\s,]*(?:on)?\\s*"+ datePattern + ")";

	  /** matches a leading line such as
	   * ----Original Message----
	   * or simply
	   * ------------------------
	   */
	  private static final String leadInLine    = "-+\\s*(?:Original(?:\\sMessage)?)?\\s*-+\n";

	  /** matches a header line indicating the date */
	  private static final String dateLine    = "(?:(?:date)|(?:sent)|(?:time)):\\s*"+ dateTimePattern + ".*\n";

	  /** matches a subject or address line */
	  private static final String subjectOrAddressLine    = "((?:from)|(?:subject)|(?:b?cc)|(?:to))|:.*\n";

	  /** matches gmail style quoted text beginning, i.e.
	   * On Mon Jun 7, 2010 at 8:50 PM, Simon wrote:
	   */
	  private static final String gmailQuotedTextBeginning = "(On\\s+" + dateTimePattern + ".*wrote:\n)";


	  /** matches the start of a quoted section of an email */
	  private static final Pattern QUOTED_TEXT_BEGINNING = Pattern.compile("(?i)(?:(?:" + leadInLine + ")?" +
	                                                                        "(?:(?:" +subjectOrAddressLine + ")|(?:" + dateLine + ")){2,6})|(?:" +
	                                                                        gmailQuotedTextBeginning + ")"
	                                                                      );
	@Override
	public void executeDataPoint(StreamRecord<EmailMsg> record) {
		String rawContent = record.getData().getContent();
		Matcher matcher = QUOTED_TEXT_BEGINNING.matcher(rawContent);
		if(matcher.find()){
			 String strippedContent = rawContent.substring(0, matcher.start());
			 record.getData().setContent(strippedContent);
		}
		this.getState().emit(record); 
		
	}

}
