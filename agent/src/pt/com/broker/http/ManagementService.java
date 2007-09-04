package pt.com.broker.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.mina.common.ByteBuffer;
import org.caudexorigo.text.StringUtils;
import org.safehaus.asyncweb.common.DefaultHttpResponse;
import org.safehaus.asyncweb.common.HttpRequest;
import org.safehaus.asyncweb.common.HttpResponseStatus;
import org.safehaus.asyncweb.common.MutableHttpResponse;
import org.safehaus.asyncweb.common.content.ByteBufferContent;
import org.safehaus.asyncweb.service.HttpService;
import org.safehaus.asyncweb.service.HttpServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.messaging.AgentInfo;

public class ManagementService implements HttpService
{
	private static final Logger log = LoggerFactory.getLogger(ManagementService.class);

	private byte[] index_page;

	private byte[] ok_page;

	private byte[] error_page;
	
	private static final byte[] footer = String.format("<!-- version:%s -->", AgentInfo.AGENT_VERSION).getBytes();
	
	

	/**
	 * Sends the configured message as an HTTP response
	 * 
	 * @param request
	 *            The request
	 */

	public ManagementService()
	{
		index_page = readHtmlPages("index.html");
		ok_page = readHtmlPages("ok.html");
		error_page = readHtmlPages("error.html");
	}

	private byte[] readHtmlPages(String pageName) throws RuntimeException
	{
		InputStream in_ix = this.getClass().getResourceAsStream(pageName);
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		int b;
		try
		{
			while ((b = in_ix.read()) > -1)
			{
				bout.write(b);
			}
			return bout.toByteArray();
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public void handleRequest(HttpServiceContext context)
	{
		MutableHttpResponse response = new DefaultHttpResponse();
		HttpRequest request = context.getRequest();
		ByteBuffer bbo = ByteBuffer.allocate(1024);
		bbo.setAutoExpand(true);

		OutputStream out = bbo.asOutputStream();

		try
		{
			response.setHeader("Pragma", "no-cache");
			response.setHeader("Cache-Control", "no-cache");
			response.setContentType("text/html");

			String loglevel = request.getParameter("loglevel");
			String logcategory = request.getParameter("logcategory");

			response.setStatus(HttpResponseStatus.OK);

			if ((StringUtils.isNotBlank(loglevel)) && (StringUtils.isNotBlank(logcategory)))
			{
				//org.apache.log4j.Logger.getLogger(logcategory).setLevel(Level.toLevel(loglevel));
				out.write(ok_page);
			}
			else
			{
				out.write(index_page);
			}
			
			out.write(footer);
		}
		catch (Throwable e)
		{

			response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
			log.error("HTTP Service error, cause:" + e.getMessage() + " client:" + context.getRemoteAddress());
			try
			{
				out.write(error_page);
			}
			catch (IOException io)
			{
			}

		}
		finally
		{
			bbo.flip();
			response.setContent(new ByteBufferContent(bbo));
			context.commitResponse(response);
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
