package pt.com.text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateUtil
{

	private static final TimeZone timeZone = TimeZone.getTimeZone("UTC");
	private static final String pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
	private static final SimpleDateFormat format = new SimpleDateFormat(pattern);
	
	static
	{
		format.setTimeZone(timeZone);
	}

	public static String formatISODate(Date date)
	{	
		return format.format(date);
	}

	/**
	 * Returns a date parsed from a date/dateTime string formatted accorings to
	 * ISO 8601 rules.
	 * 
	 * @param date
	 *            the formatted date/dateTime string.
	 * @return the parsed date.
	 * @throws ParseException
	 *             if the date/dateTime string could not be parsed.
	 */
	public static Date parseISODate(String date) throws ParseException
	{
		String pattern;
		StringBuilder buffer = new StringBuilder(date);
		switch (buffer.length())
		{
		case 4:
			// Year: yyyy (eg 1997)
			pattern = "yyyy";
			break;
		case 7:
			// Year and month: yyyy-MM (eg 1997-07)
			pattern = "yyyy-MM";
			break;
		case 10:
			// Complete date: yyyy-MM-dd (eg 1997-07-16)
			pattern = "yyyy-MM-dd";
			break;
		default:
			// Complete date plus hours and minutes: yyyy-MM-ddTHH:mmTZD (eg
			// 1997-07-16T19:20+01:00)
			// Complete date plus hours, minutes and seconds:
			// yyyy-MM-ddTHH:mm:ssTZD (eg 1997-07-16T19:20:30+01:00)
			// Complete date plus hours, minutes, seconds and a decimal fraction
			// of a second: yyyy-MM-ddTHH:mm:ss.STZD (eg
			// 1997-07-16T19:20:30.45+01:00)
			pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
			if (buffer.length() == 16)
			{
				// add seconds
				buffer.append(":00");
			}
			if (buffer.length() > 16 && buffer.charAt(16) != ':')
			{
				// insert seconds
				buffer.insert(16, ":00");
			}
			if (buffer.length() == 19)
			{
				// add milliseconds
				buffer.append(".000");
			}
			if (buffer.length() > 19 && buffer.charAt(19) != '.')
			{
				// insert milliseconds
				buffer.insert(19, ".000");
			}
			if (buffer.length() == 23)
			{
				// append timzeone
				buffer.append("+0000");
			}
			if (buffer.length() == 24 && buffer.charAt(23) == 'Z')
			{
				// replace 'Z' with '+0000'
				buffer.replace(23, 24, "+0000");
			}
			if (buffer.length() == 29 && buffer.charAt(26) == ':')
			{
				// delete '.' from 'HH:mm'
				buffer.deleteCharAt(26);
			}
		}
		// always set time zone on formatter
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		format.setTimeZone(timeZone);
		return format.parse(buffer.toString());
	}

}
