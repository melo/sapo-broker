package pt.com.broker.messaging;

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
}
