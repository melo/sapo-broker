package pt.com.broker.messaging;

import java.util.Date;

import org.caudexorigo.text.DateUtil;
import org.caudexorigo.text.StringUtils;

public class BrokerMessage
{
	public int priority;

	public String messageId;

	public String correlationId;

	public String timestamp;

	public String expiration;

	public String destinationName;

	public String textPayload;

	public BrokerMessage()
	{
	}

	public Date expirationAsDate()
	{
		return convertToDate(expiration);
	}

	public Date timestampAsDate()
	{
		return convertToDate(timestamp);
	}

	public void expirationFromDate(Date date)
	{
		expiration = DateUtil.formatISODate(date);
	}

	private Date convertToDate(String dateString)
	{
		try
		{
			if (StringUtils.isNotBlank(dateString))
			{
				return DateUtil.parseISODate(dateString);
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
