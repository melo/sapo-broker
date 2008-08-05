package pt.com.broker.http;

import org.apache.mina.filter.codec.http.HttpRequest;

import pt.com.http.HttpAction;
import pt.com.http.RequestRouter;

public class BrokerRequestRouter implements RequestRouter
{
	private final BrokerHttpAction broker_action = new BrokerHttpAction();
	
	private final StatusAction status_action = new StatusAction();
	
	private final AdminAction admin_action = new AdminAction();

	public HttpAction map(HttpRequest req)
	{
		String path = req.getRequestUri().getPath();
		if (path.equals("/broker/producer"))
		{
			return broker_action;
		}
		else if (path.equals("/broker/status"))
		{
			return status_action;
		}
		else if (path.equals("/broker/admin"))
		{
			return admin_action;
		}
		return null;
	}

}