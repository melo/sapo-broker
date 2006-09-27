package pt.com.broker;

import org.apache.mina.common.IoSession;

public abstract class WriterTask implements Runnable
{
	private IoSession _ioSession;
		
	public IoSession getIoSession()
	{
		return _ioSession;
	}

	public void setIoSession(IoSession session)
	{
		_ioSession = session;
	}
}
