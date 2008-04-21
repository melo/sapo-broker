package pt.com.broker.messaging;

import java.util.Date;

import org.caudexorigo.text.DateUtil;

import pt.com.gcs.conf.AgentInfo;

public class Status
{
	public String message;

	public String timestamp;

	public Status()
	{
		message = String.format("Agent '%s' is alive", AgentInfo.getAgentName());
		timestamp = DateUtil.formatISODate(new Date());
	}

}
