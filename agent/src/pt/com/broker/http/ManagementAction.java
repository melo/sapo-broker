package pt.com.broker.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.http.HttpRequest;
import org.apache.mina.filter.codec.http.HttpResponseStatus;
import org.apache.mina.filter.codec.http.MutableHttpResponse;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.messaging.AgentInfo;
import pt.com.http.HttpAction;

public class ManagementAction extends HttpAction
{
	private static final Logger log = LoggerFactory.getLogger(ManagementAction.class);

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

	public ManagementAction()
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

	@Override
	public void writeResponse(IoSession iosession, HttpRequest request, MutableHttpResponse response)
	{
		IoBuffer bbo = IoBuffer.allocate(1024);
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
				// org.apache.log4j.Logger.getLogger(logcategory).setLevel(Level.toLevel(loglevel));
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
			log.error("HTTP Service error, cause:" + e.getMessage() + " client:" + iosession.getRemoteAddress());
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
			response.setContent(bbo);
		}

	}

}
