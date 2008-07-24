package pt.com.broker.http;

import java.io.OutputStream;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.http.HttpMethod;
import org.apache.mina.filter.codec.http.HttpRequest;
import org.apache.mina.filter.codec.http.HttpResponseStatus;
import org.apache.mina.filter.codec.http.MutableHttpResponse;
import org.caudexorigo.Shutdown;
import org.caudexorigo.io.UnsynchByteArrayInputStream;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.core.ErrorHandler;
import pt.com.broker.messaging.BrokerProducer;
import pt.com.broker.messaging.MQ;
import pt.com.broker.xml.SoapEnvelope;
import pt.com.broker.xml.SoapSerializer;
import pt.com.gcs.messaging.QueueProcessorList;
import pt.com.http.HttpAction;

public class BrokerHttpAction extends HttpAction
{
	private static final String content_type = "text/xml";

	private static final Logger log = LoggerFactory.getLogger(BrokerHttpAction.class);

	private static final long serialVersionUID = 9072384515868129239L;

	private static final BrokerProducer _http_broker = BrokerProducer.getInstance();

	private IoBuffer BAD_REQUEST_RESPONSE;

	public BrokerHttpAction()
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

				SoapEnvelope req_message = SoapSerializer.FromXml(new UnsynchByteArrayInputStream(buf));
				String requestSource = MQ.requestSource(req_message);

				if (req_message.body.publish != null)
				{
					String destinationName = req_message.body.publish.brokerMessage.destinationName;
					
					if (StringUtils.equals(destinationName, "/system/management"))
					{
						String payload = req_message.body.publish.brokerMessage.textPayload;
						
						if (StringUtils.isNotBlank(payload))
						{
							if (payload.equals("RELOAD"))
							{
								Shutdown.now();
							}
							else if (payload.startsWith("QUEUE:"))
							{
								String queueName = StringUtils.substringAfter(payload, ":");
								QueueProcessorList.remove(queueName);
							}
						}
					}
					else
					{
						_http_broker.publishMessage(req_message.body.publish, requestSource);
					}
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
				response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
				response.setContent(BAD_REQUEST_RESPONSE.duplicate());
			}
		}
		catch (Throwable e)
		{
			response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
			fault(null, e, response);			
			if (log.isErrorEnabled())
			{
				log.error("HTTP Service error, cause:" + e.getMessage() + ". Client address:" + session.getRemoteAddress());
			}
		}
	}

	public void fault(String faultCode, Throwable cause, MutableHttpResponse response)
	{
		IoBuffer bbf = IoBuffer.allocate(1024);
		bbf.setAutoExpand(true);
		OutputStream out = bbf.asOutputStream();

		SoapEnvelope ex_msg = ErrorHandler.buildSoapFault(faultCode, cause).Message;
		SoapSerializer.ToXml(ex_msg, out);
		bbf.flip();
		response.setContentType(content_type);
		response.setContent(bbf);
	}

}
