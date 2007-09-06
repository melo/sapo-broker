package pt.com.gcs.net;

public class Peer
{

	private String _name;

	private final String _host;

	private final int _port;

	public Peer(String name, String host, int port)
	{
		_name = name;
		_host = host;
		_port = port;
	}

	public String getName()
	{
		return _name;
	}

	public String getHost()
	{
		return _host;
	}

	public int getPort()
	{
		return _port;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_host == null) ? 0 : _host.hashCode());
		result = prime * result + ((_name == null) ? 0 : _name.hashCode());
		result = prime * result + _port;
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final Peer other = (Peer) obj;
		if (_host == null)
		{
			if (other._host != null)
				return false;
		}
		else if (!_host.equals(other._host))
			return false;
		if (_name == null)
		{
			if (other._name != null)
				return false;
		}
		else if (!_name.equals(other._name))
			return false;
		if (_port != other._port)
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		// TODO Auto-generated method stub
		return _name + "#" + _host + ":" + _port;
	}
}
