package pt.com.gcs.messaging;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TopicMatcher
{
	private static Logger log = LoggerFactory.getLogger(TopicMatcher.class);

	protected static boolean match(String subscriptionName, String topicName)
	{
		try
		{
			Pattern p = PatternCache.get(subscriptionName);
			Matcher m = p.matcher(topicName);
			return m.matches();
		}
		catch (Throwable t)
		{
			String message = String.format("match-> subscriptionName: '%s'; topicName: '%s'", subscriptionName, topicName);
			log.error(message, t);
			return false;
		}
	}
}
