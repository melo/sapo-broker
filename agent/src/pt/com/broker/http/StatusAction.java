package pt.com.broker.http;

import java.io.OutputStream;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.http.HttpRequest;
import org.apache.mina.filter.codec.http.HttpResponseStatus;
import org.apache.mina.filter.codec.http.MutableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.messaging.Status;
import pt.com.http.HttpAction;

public class StatusAction extends HttpAction
{
	private static final Logger log = LoggerFactory.getLogger(StatusAction.class);

	private static final String template = "<mq:Status xmlns:mq=\"http://services.sapo.pt/broker\">%n<mq:Message>%s</mq:Message>%n<mq:Timestamp>%s</mq:Timestamp>%n<mq:Version>%s</mq:Version>%n</mq:Status>";

	public StatusAction()
	{
	}

	@Override
	public void writeResponse(IoSession iosession, HttpRequest request, MutableHttpResponse response)
	{
		IoBuffer bbo = IoBuffer.allocate(1024);
		bbo.setAutoExpand(true);

		OutputStream out = bbo.asOutputStream();

		try
		{
			Status status = new Status();

			String smessage = String.format(template, status.message, status.timestamp, status.version);
			byte[] bmessage = smessage.getBytes("UTF-8");
			response.setHeader("Pragma", "no-cache");
			response.setHeader("Cache-Control", "no-cache");
			response.setContentType("text/xml");

			response.setStatus(HttpResponseStatus.OK);

			out.write(bmessage);
		}
		catch (Throwable e)
		{
			response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
			log.error("HTTP Service error, cause:" + e.getMessage() + " client:" + iosession.getRemoteAddress());
		}
		finally
		{
			bbo.flip();
			response.setContent(bbo);
		}

	}

}
