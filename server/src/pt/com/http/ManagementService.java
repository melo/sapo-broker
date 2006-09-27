package pt.com.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Level;
import org.safehaus.asyncweb.http.HttpRequest;
import org.safehaus.asyncweb.http.HttpResponse;
import org.safehaus.asyncweb.http.HttpService;
import org.safehaus.asyncweb.http.ResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.text.StringUtils;

public class ManagementService implements HttpService
{
	private static final Logger log = LoggerFactory.getLogger(ManagementService.class);

	private byte[] index_page;

	private byte[] ok_page;

	private byte[] error_page;

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

	public void handleRequest(HttpRequest request)
	{
		HttpResponse response = request.createHttpResponse();
		OutputStream out = response.getOutputStream();

		try
		{
			response.setHeader("Pragma", "no-cache");
			response.setHeader("Cache-Control", "no-cache");
			response.setContentType("text/html");

			String loglevel = request.getParameter("loglevel");
			String logcategory = request.getParameter("logcategory");

			response.setStatus(ResponseStatus.OK);

			if ((StringUtils.isNotBlank(loglevel)) && (StringUtils.isNotBlank(logcategory)))
			{
				org.apache.log4j.Logger.getLogger(logcategory).setLevel(Level.toLevel(loglevel));
				out.write(ok_page);
			}
			else
			{
				out.write(index_page);
			}

		}
		catch (Throwable e)
		{

			response.setStatus(ResponseStatus.INTERNAL_SERVER_ERROR);
			log.error("HTTP Service error, cause:" + e.getMessage() + " client:" + request.getRemoteAddress());
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
