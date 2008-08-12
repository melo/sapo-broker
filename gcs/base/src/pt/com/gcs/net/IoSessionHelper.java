package pt.com.gcs.net;

import java.net.SocketAddress;

import org.apache.mina.core.session.IoSession;

public class IoSessionHelper
{
	private static final String REMOTE_ADDRESS_AS_STRING_ATTR = "pt.com.gcs.net.IoSessionHelper.REMOTE_ADDRESS_AS_STRING_ATTR";
	private static final String REMOTE_ADDRESS_ATTR = "pt.com.gcs.net.IoSessionHelper.REMOTE_ADDRESS_ATTR";

	public static String getRemoteAddress(IoSession iosession)
	{
		String remoteClient;
		try
		{
			remoteClient = iosession.getAttribute(REMOTE_ADDRESS_AS_STRING_ATTR).toString();
		}
		catch (Throwable e)
		{
			remoteClient = "Can't determine remote address";
		}
		return remoteClient;
	}

	public static SocketAddress getRemoteInetAddress(IoSession iosession)
	{
		SocketAddress remoteClient;
		try
		{
			remoteClient = (SocketAddress) iosession.getAttribute(REMOTE_ADDRESS_ATTR);
		}
		catch (Throwable e)
		{
			remoteClient = null;
		}
		return remoteClient;
	}

	public static void tagWithRemoteAddress(IoSession iosession)
	{
		iosession.setAttribute(REMOTE_ADDRESS_AS_STRING_ATTR, iosession.getRemoteAddress().toString());
		iosession.setAttribute(REMOTE_ADDRESS_ATTR, iosession.getRemoteAddress());
	}

}
