package pt.com.broker.core;

import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Env
{
	private static Logger log = LoggerFactory.getLogger(Env.class);

	private Env()
	{
	}

	public static int portFromSys(String sys_name_port)
	{
		int port = 0;
		String portNumber = System.getProperty(sys_name_port);

		if (StringUtils.isBlank(portNumber))
		{
			log.error("You must define the system property \"" + sys_name_port + "\"!");
			System.exit(-1);
		}

		try
		{
			port = Integer.parseInt(portNumber);
			return port;
		}
		catch (Exception e)
		{
			log.error("The system property \"" + sys_name_port + "\" is incorrectly defined, you must choose a positve integer!");
			System.exit(-1);
			return port;
		}
	}

}
