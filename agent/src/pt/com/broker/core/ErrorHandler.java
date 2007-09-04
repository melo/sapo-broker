package pt.com.broker.core;

import java.io.PrintWriter;

import org.apache.mina.common.ExceptionMonitor;
import org.jibx.runtime.JiBXException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.caudexorigo.text.StringBuilderWriter;
import org.caudexorigo.text.StringUtils;

import pt.com.broker.Start;
import pt.com.broker.xml.SoapEnvelope;
import pt.com.broker.xml.SoapFault;

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
		String reason = ex.getMessage();
		String detail = sw.toString();

		SoapEnvelope faultMessage = new SoapEnvelope();
		SoapFault sfault = new SoapFault();
		sfault.faultCode.value = faultCode;
		sfault.faultReason.text = reason;
		sfault.detail = detail;
		faultMessage.body.fault = sfault;

		WTF wtf = new WTF();
		wtf.Cause = ex;
		wtf.Message = faultMessage;

		return wtf;
	}

	public static class WTF
	{
		public SoapEnvelope Message;

		public Throwable Cause;
	}
}
