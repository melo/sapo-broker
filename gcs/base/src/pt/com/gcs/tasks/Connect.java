package pt.com.gcs.tasks;

import java.net.SocketAddress;

import pt.com.gcs.messaging.Gcs;

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