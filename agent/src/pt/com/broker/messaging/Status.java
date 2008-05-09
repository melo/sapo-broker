package pt.com.broker.messaging;

import java.util.Date;

import org.caudexorigo.text.DateUtil;

import pt.com.broker.core.BrokerInfo;
import pt.com.gcs.conf.GcsInfo;

public class Status
{
	public String message;

	public String timestamp;
	
	public String version;

	public Status()
	{
		message = String.format("Agent '%s' is alive", GcsInfo.getAgentName());
		timestamp = DateUtil.formatISODate(new Date());
		version = BrokerInfo.VERSION;
	}

}
