package pt.com.broker.http;

import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.http.HttpMethod;
import org.apache.mina.filter.codec.http.HttpRequest;
import org.apache.mina.filter.codec.http.HttpResponseStatus;
import org.apache.mina.filter.codec.http.MutableHttpResponse;
import org.caudexorigo.ErrorAnalyser;
import org.caudexorigo.Shutdown;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.core.BrokerExecutor;
import pt.com.gcs.messaging.Gcs;
import pt.com.http.HttpAction;

public class AdminAction extends HttpAction
{
	private static final String content_type = "text/plain";

	private static final Logger log = LoggerFactory.getLogger(AdminAction.class);
	private IoBuffer BAD_REQUEST_RESPONSE;

	public AdminAction()
	{
		super();
		try
		{
			byte[] bad_arr = "<p>Only the POST verb is supported</p>".getBytes("UTF-8");
			BAD_REQUEST_RESPONSE = IoBuffer.allocate(bad_arr.length);
			BAD_REQUEST_RESPONSE.put(bad_arr);
			BAD_REQUEST_RESPONSE.flip();
		}
		catch (Throwable error)
		{
			log.error("Fatal JVM error!", error);
			Shutdown.now();
		}
	}

	@Override
	public void writeResponse(IoSession session, HttpRequest request, MutableHttpResponse response)
	{
		try
		{
			if (request.getMethod().equals(HttpMethod.POST))
			{
				IoBuffer bb = (IoBuffer) request.getContent();
				byte[] buf = new byte[bb.limit()];
				bb.position(0);
				bb.get(buf);

				String action = new String(buf);

				if (StringUtils.isBlank(action))
				{
					throw new IllegalArgumentException("No arguments supplied");
				}

				if (action.equals("SHUTDOWN"))
				{
					Runnable kill = new Runnable()
					{
						public void run()
						{
							Shutdown.now();
						}
					};
					BrokerExecutor.schedule(kill, 1000, TimeUnit.MILLISECONDS);

				}
				else if (action.startsWith("QUEUE:"))
				{
					String from = session.getRemoteAddress().toString();
					String queueName = StringUtils.substringAfter(action, "QUEUE:");
					Gcs.deleteQueue(queueName);
					String message = String.format("Queue '%s' was deleted. Request from: '%s'%n", queueName, from);
					log.info(message);
					IoBuffer bbo = IoBuffer.allocate(1024);
					bbo.setAutoExpand(true);
					OutputStream out = bbo.asOutputStream();
					out.write(message.getBytes("UTF-8"));
					bbo.flip();
					response.setContent(bbo);
				}

				response.setStatus(HttpResponseStatus.OK);
			}
			else
			{
				response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
				response.setContent(BAD_REQUEST_RESPONSE.duplicate());
			}
		}
		catch (Throwable e)
		{
			response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
			fault(e, response);
			if (log.isErrorEnabled())
			{
				log.error("HTTP Service error, cause:" + e.getMessage() + ". Client address:" + session.getRemoteAddress());
			}
		}
	}

	public void fault(Throwable cause, MutableHttpResponse response)
	{
		try
		{
			IoBuffer bbf = IoBuffer.allocate(1024);
			bbf.setAutoExpand(true);
			OutputStream out = bbf.asOutputStream();
			Throwable rootCause = ErrorAnalyser.findRootCause(cause);
			out.write(("Error: " + rootCause.getMessage() + "\n").getBytes("UTF-8"));
			bbf.flip();
			response.setContentType(content_type);
			response.setContent(bbf);
		}
		catch (Throwable e)
		{
			// ignore
		}

	}

}
