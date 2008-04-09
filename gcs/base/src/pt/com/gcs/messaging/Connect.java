package pt.com.gcs.messaging;

import java.net.SocketAddress;


public class Connect implements Runnable
{
	private SocketAddress _address;

	public Connect(SocketAddress address)
	{
		_address = address;
	}

	public void run()
	{
		Gcs.connect(_address);
	}
}