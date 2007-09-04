package pt.com.broker.http;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoFutureListener;
import org.apache.mina.common.IoHandler;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.WriteFuture;
import org.caudexorigo.io.UnsynchByteArrayInputStream;
import org.safehaus.asyncweb.common.DefaultHttpResponse;
import org.safehaus.asyncweb.common.HttpMethod;
import org.safehaus.asyncweb.common.HttpRequest;
import org.safehaus.asyncweb.common.HttpResponseStatus;
import org.safehaus.asyncweb.common.MutableHttpResponse;
import org.safehaus.asyncweb.common.content.ByteBufferContent;
import org.safehaus.asyncweb.util.HttpHeaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.core.ErrorHandler;
import pt.com.broker.messaging.BrokerProducer;
import pt.com.broker.messaging.MQ;
import pt.com.broker.xml.SoapEnvelope;
import pt.com.broker.xml.SoapSerializer;

public class HttpProtocolHandler implements IoHandler
{

	private static final Logger log = LoggerFactory.getLogger(HttpProtocolHandler.class);

	private static final long serialVersionUID = 9072384515868129239L;

	private static final BrokerProducer _http_broker = BrokerProducer.getInstance();

	private static final byte[] GET_RESPONSE = "<p>Only the POST verb is supported</p>".getBytes();

	public HttpProtocolHandler()
	{
	}

	public void exceptionCaught(IoSession session, Throwable cause) throws Exception
	{
		if (!(cause instanceof IOException))
		{
			log.warn("Unexpected exception: {}", cause.getMessage());
		}
		session.close();
	}

	public void fault(String faultCode, Throwable cause, MutableHttpResponse response)
	{
		ByteBuffer bbf = ByteBuffer.allocate(1024);
		bbf.setAutoExpand(true);
		OutputStream out = bbf.asOutputStream();

		SoapEnvelope ex_msg = ErrorHandler.buildSoapFault(faultCode, cause).Message;
		SoapSerializer.ToXml(ex_msg, out);
		bbf.flip();
		response.setContent(new ByteBufferContent(bbf));
	}

	public void messageReceived(IoSession session, Object message) throws Exception
	{
		HttpRequest req = (HttpRequest) message;
		String path = req.getRequestUri().getPath();

		if (path.endsWith("/broker/producer"))
		{
			handleRequest(session, req);
		}
		else
		{
			MutableHttpResponse res = new DefaultHttpResponse();
			res.setStatus(HttpResponseStatus.NOT_FOUND);
			writeResponse(session, req, res);
		}
	}

	/**
	 * Sends the configured message as an HTTP response
	 * 
	 * @param request
	 *            The request
	 */
	public void handleRequest(IoSession session, HttpRequest request)
	{
		MutableHttpResponse response = new DefaultHttpResponse();

		try
		{
			if (request.getMethod().equals(HttpMethod.POST))
			{
				ByteBufferContent bbci = (ByteBufferContent) request.getContent();
				ByteBuffer bb = bbci.getByteBuffer();
				byte[] buf = new byte[bb.limit()];
				bb.position(0);
				bb.get(buf);

				SoapEnvelope req_message = SoapSerializer.FromXml(new UnsynchByteArrayInputStream(buf));
				String requestSource = MQ.requestSource(req_message);

				if (req_message.body.publish != null)
				{
					_http_broker.publishMessage(req_message.body.publish, requestSource);
				}
				else if (req_message.body.enqueue != null)
				{
					_http_broker.enqueueMessage(req_message.body.enqueue, requestSource);
				}
				else
				{
					response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
					fault("soap:Sender", new UnsupportedOperationException(), response);
					return;
				}

				response.setStatus(HttpResponseStatus.ACCEPTED);
			}
			else
			{
				response.setStatus(HttpResponseStatus.BAD_REQUEST);
				ByteBuffer bb = ByteBuffer.allocate(GET_RESPONSE.length);
				bb.setAutoExpand(false);
				bb.put(GET_RESPONSE);
				bb.flip();
				response.setContent(new ByteBufferContent(bb));
			}
		}
		catch (Throwable e)
		{
			fault(null, e, response);
			response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
			if (log.isErrorEnabled())
			{
				log.error("HTTP Service error, cause:" + e.getMessage() + ". Client address:" + session.getRemoteAddress());
			}
		}
		finally
		{
			writeResponse(session, request, response);
		}
	}

	protected void writeResponse(IoSession session, HttpRequest req, MutableHttpResponse res)
	{
		res.normalize(req);
		WriteFuture future = session.write(res);
		if (!HttpHeaderConstants.VALUE_KEEP_ALIVE.equalsIgnoreCase(res.getHeader(HttpHeaderConstants.KEY_CONNECTION)))
		{
			future.addListener(IoFutureListener.CLOSE);
		}
	}

	public void messageSent(IoSession session, Object message) throws Exception
	{
	}

	public void sessionClosed(IoSession session) throws Exception
	{
	}

	public void sessionCreated(IoSession session) throws Exception
	{
	}

	public void sessionIdle(IoSession session, IdleStatus status) throws Exception
	{
		session.close();
	}

	public void sessionOpened(IoSession session) throws Exception
	{
	}
}
