package pt.com.manta;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.mina.common.ExceptionMonitor;
import org.jibx.runtime.JiBXException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.text.StringBuilderWriter;
import pt.com.text.StringEscapeUtils;
import pt.com.text.StringUtils;
import pt.com.xml.SoapConstants;

public class ErrorHandler extends ExceptionMonitor
{
	private static final Logger log = LoggerFactory.getLogger(ErrorHandler.class);

	public void exceptionCaught(Throwable cause)
	{
		Throwable rootCause = findRootCause(cause);
		if (log.isWarnEnabled())
		{
			log.error("Unexpected exception.", rootCause);
		}
		exitIfOOM(rootCause);
	}

	public static Throwable findRootCause(Throwable ex)
	{
		Throwable error_ex = new Exception(ex);
		while (error_ex.getCause() != null)
		{
			error_ex = error_ex.getCause();
		}
		return error_ex;
	}

	public static void exitIfOOM(Throwable t)
	{
		if (t instanceof OutOfMemoryError)
		{
			log.error("Giving up, reason: " + t.getMessage());
			Start.shutdown();
		}
	}


	public static void checkAbort(Throwable t)
	{
		Throwable rootCause = findRootCause(t);
		exitIfOOM(rootCause);
	}

	public static WTF buildSoapFault(Throwable ex)
	{
		Throwable rootCause = findRootCause(ex);
		exitIfOOM(rootCause);

		String ereason = "soap:Receiver";
		if (rootCause instanceof JiBXException)
		{
			ereason = "soap:Sender";
		}
		else if (rootCause instanceof IllegalArgumentException)
		{
			ereason = "soap:Sender";
		}

		return _buildSoapFault(ereason, rootCause);
	}

	public static WTF buildSoapFault(String faultCode, Throwable ex)
	{
		if (StringUtils.isBlank(faultCode))
		{
			return buildSoapFault(ex);
		}

		Throwable rootCause = findRootCause(ex);
		exitIfOOM(rootCause);
		return _buildSoapFault(faultCode, rootCause);
	}

	private static WTF _buildSoapFault(String faultCode, Throwable ex)
	{
		StringBuilderWriter sw = new StringBuilderWriter();
		PrintWriter pw = new PrintWriter(sw);
		ex.printStackTrace(pw);
		String reason = StringEscapeUtils.escapeXml(ex.getMessage());
		String detail = StringEscapeUtils.escapeXml(sw.toString());

		StringBuilder sb = new StringBuilder();
		sb.append(SoapConstants.soap_prefix);
		sb.append("\n<soap:Fault>");
		sb.append("\n<soap:Code>\n<soap:Value>" + faultCode + "</soap:Value>\n</soap:Code>");
		sb.append("\n<soap:Reason>\n<soap:Text xml:lang='en-US'>" + reason + "</soap:Text>\n</soap:Reason>");
		sb.append("\n<soap:Detail>\n" + detail + "\n</soap:Detail>");
		sb.append("\n</soap:Fault>");
		sb.append(SoapConstants.soap_suffix);

		WTF wtf = new WTF();
		wtf.Cause = ex;
		try
		{
			wtf.Message = sb.toString().getBytes("UTF-8");
		}
		catch (IOException e)
		{
			wtf.Message = new byte[0];
		}

		return wtf;
	}

	public static class WTF
	{
		public byte[] Message;

		public Throwable Cause;
	}
}
