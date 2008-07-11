package pt.com.broker.client.messaging;

import java.util.Date;

import org.caudexorigo.text.DateUtil;
import org.caudexorigo.text.StringUtils;

public class Status
{
	public String message;

	public String timestamp;
	
	public String version;

	public Status()
	{
		message = "";
		timestamp = "";
		version = "";
	}

	public Date timestampAsDate()
	{
		try
		{
			if (StringUtils.isNotBlank(timestamp))
			{
				return DateUtil.parseISODate(timestamp);
			}
			else
			{
				throw new RuntimeException("Invalid date format");
			}
		}
		catch (Throwable t)
		{
			throw new RuntimeException(t);
		}
	}
	
	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		sb.append("message:" + message);
		sb.append("; timestamp:" + timestamp);
		sb.append("; version:" + version);
		sb.append("}");
		return sb.toString();
	}
}
