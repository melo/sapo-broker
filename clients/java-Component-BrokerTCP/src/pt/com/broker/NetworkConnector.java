package pt.com.broker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import org.caudexorigo.concurrent.Sleep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkConnector
{
	private static final Logger log = LoggerFactory.getLogger(NetworkConnector.class);

	private final String _host;
	private final int _port;

	private Socket _client;
	private DataInputStream _rawi = null;
	private DataOutputStream _rawo = null;
	private SocketAddress _addr;
	private String _saddr;

	public NetworkConnector(String host, int port) throws UnknownHostException, IOException
	{
		_host = host;
		_port = port;

		_client = new Socket(_host, _port);
		_client.setSoTimeout(0);
		_rawo = new DataOutputStream(_client.getOutputStream());
		_rawi = new DataInputStream(_client.getInputStream());

		_addr = _client.getRemoteSocketAddress();
		_saddr = _addr.toString();

		log.info("Receive Buffer Size: " + _client.getReceiveBufferSize());
		log.info("Send Buffer Size: " + _client.getSendBufferSize());
	}

	public void reconnect(Throwable se)
	{
		log.warn("Connect Error: " + se.getMessage());
		
		close();
		
		Throwable ex = new Exception(se);

		while (ex != null)
		{			
			try
			{
				log.error("Trying to reconnect");
				_client = new Socket(_host, _port);
				_rawo = new DataOutputStream(_client.getOutputStream());
				_rawi = new DataInputStream(_client.getInputStream());
				_addr = _client.getRemoteSocketAddress();
				_saddr = _addr.toString();

				ex = null;
				log.info("Connection established: " + _saddr);

			}
			catch (Exception re)
			{
				log.info("Reconnect failled");
				ex = re;
				Sleep.time(2000);
			}
		}
	}

	public DataInputStream getInput()
	{
		return _rawi;
	}

	public DataOutputStream getOutput()
	{
		return _rawo;
	}

	public void close()
	{
		try
		{
			_rawi.close();
		}
		catch (Throwable e)
		{
		}

		try
		{
			_rawo.close();
		}
		catch (Throwable e)
		{
		}

		try
		{
			_client.close();
		}
		catch (Throwable e)
		{
		}
	}

	public boolean isConnected()
	{
		return _client.isConnected();
	}

	public boolean isInputShutdown()
	{
		return _client.isInputShutdown();
	}

	public boolean isOutputShutdown()
	{
		return _client.isOutputShutdown();
	}

	public SocketAddress getInetAddress()
	{
		return _addr;
	}

	public String getAddress()
	{
		return _saddr;
	}

}
