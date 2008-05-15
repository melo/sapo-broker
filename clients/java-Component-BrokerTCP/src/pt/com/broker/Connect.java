package pt.com.broker;

import java.net.SocketAddress;

public class Connect implements Runnable
{
	private NetworkHandler _netHandler;
	private SocketAddress _address;

	public Connect(NetworkHandler netHandler, SocketAddress address)
	{
		_netHandler = netHandler;
		_address = address;
	}

	public void run()
	{
		_netHandler.connect(_netHandler.getConnector(), _address);
	}
}