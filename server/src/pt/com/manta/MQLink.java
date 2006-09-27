package pt.com.manta;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import org.mr.api.jms.MantaConnectionFactory;

public class MQLink
{

	private MantaConnectionFactory _factory;

	private static final MQLink instance = new MQLink();

	private MQLink()
	{
		initJMS();
	}

	private void initJMS()
	{
		try
		{
			_factory = new MantaConnectionFactory();

		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static ConnectionFactory getJMSConnectionFactory()
	{
		return instance._factory;
	}
	
	public static Connection getJMSConnection()
	{
		try
		{
			Connection con = instance._factory.createConnection();
			con.start();
			return con;
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
}
