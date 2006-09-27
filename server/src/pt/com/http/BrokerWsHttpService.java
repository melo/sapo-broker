package pt.com.http;

import java.io.IOException;
import java.io.OutputStream;

import org.safehaus.asyncweb.http.HttpRequest;
import org.safehaus.asyncweb.http.HttpResponse;
import org.safehaus.asyncweb.http.HttpService;
import org.safehaus.asyncweb.http.ResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.BrokerProducer;
import pt.com.broker.MQ;
import pt.com.manta.ErrorHandler;
import pt.com.xml.SoapConstants;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapSerializer;

public class BrokerWsHttpService implements HttpService
{
	private static final Logger log = LoggerFactory.getLogger(BrokerWsHttpService.class);

	private static final long serialVersionUID = 9072384515868129239L;

	private static final byte[] b_soap_prefix = SoapConstants.soap_prefix.getBytes();

	private static final byte[] b_soap_suffix = SoapConstants.soap_suffix.getBytes();

	private static final BrokerProducer _http_broker = BrokerProducer.getInstance();

	public void fault(String faultCode, OutputStream out, Throwable cause)
	{
		byte[] ex_msg = ErrorHandler.buildSoapFault(faultCode, cause).Message;
		try
		{
			out.write(ex_msg);
		}
		catch (IOException e)
		{
			// IGNORE;
		}
	}

	/**
	 * Sends the configured message as an HTTP response
	 * 
	 * @param request
	 *            The request
	 */
	public void handleRequest(HttpRequest request)
	{
		HttpResponse response = request.createHttpResponse();
		OutputStream out = response.getOutputStream();

		try
		{
			response.setHeader("Pragma", "no-cache");
			response.setHeader("Cache-Control", "no-cache");
			response.setContentType("application/soap+xml");

			SoapEnvelope req_message = SoapSerializer.FromXml(request.getInputStream());

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
				response.setStatus(ResponseStatus.INTERNAL_SERVER_ERROR);
				fault("soap:Sender", out, new UnsupportedOperationException());
				return;
			}

			response.setStatus(ResponseStatus.OK);
			out.write(b_soap_prefix);
			out.write(b_soap_suffix);
		}
		catch (Throwable e)
		{
			fault(null, out, e);
			response.setStatus(ResponseStatus.INTERNAL_SERVER_ERROR);
			if (log.isErrorEnabled())
			{
				log.error("HTTP Service error, cause:" + e.getMessage() + ". Client address:" + request.getRemoteAddress());
			}
		}
		finally
		{
			request.commitResponse(response);
		}
	}

	public void start()
	{
		// Dont care
	}

	public void stop()
	{
		// Dont care
	}
}
