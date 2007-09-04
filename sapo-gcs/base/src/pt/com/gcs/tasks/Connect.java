package pt.com.gcs.tasks;

import pt.com.gcs.Gcs;
import pt.com.gcs.net.Peer;

public class Connect implements Runnable
{
	private Peer _peer;
	
	public Connect(Peer peer)
	{
		_peer = peer;
	}

	public void run()
	{
		Gcs.connect(_peer.getHost(), _peer.getPort());
	}
}
