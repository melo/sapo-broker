package pt.com.gcs.conf;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.caudexorigo.io.FilenameUtils;
import org.caudexorigo.text.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.Shutdown;

public class AgentInfo
{
	private static Logger log = LoggerFactory.getLogger(AgentInfo.class);

	public static final String AGENT_VERSION = "2.0";

	private static final AgentInfo instance = new AgentInfo();

	public static String getAgentHost()
	{
		String prop = instance.conf.getNet().getIp();
		if (StringUtils.isBlank(prop))
		{
			log.error("Fatal error: Must define valid host.");
			Shutdown.now();
		}
		return prop;
	}

	public static String getAgentName()
	{
		String prop = instance.conf.getName();
		if (StringUtils.isBlank(prop))
		{
			log.error("Fatal error: Must define an Agent name.");
			Shutdown.now();
		}
		return prop;
	}

	public static int getAgentPort()
	{
		int iprop = instance.conf.getNet().getPort();
		return iprop;
	}

	public static String getBasePersistentDirectory()
	{
		String prop = instance.conf.getPersistency().getDirectory();
		String defaultDir = FilenameUtils.normalizeNoEndSeparator(System.getProperty("user.dir")) + File.separator + "persistent";
		if (StringUtils.isBlank(prop))
		{
			log.warn("No directory for persistent storage. Using default: {}", defaultDir);
			return defaultDir;
		}
		else
		{
			return FilenameUtils.normalizeNoEndSeparator(prop);
		}
	}

	public static long getSegmentSize()
	{

		long lprop = instance.conf.getNet().getPort();
		return lprop;
	}

	public static int getInitialDelay()
	{
		int iprop = instance.conf.getNet().getDiscoveryDelay();
		return iprop;
	}

	public static String getStatisticsTopic()
	{
		String prop = instance.conf.getStatistics().getTopic();
		return prop;
	}

	public static String getConfigVersion()
	{
		String prop = instance.conf.getConfigVersion();
		return prop;
	}

	public static String getWorldMapPath()
	{
		String prop = instance.conf.getNet().getFileRef();
		if (StringUtils.isBlank(prop))
		{
			log.error("Fatal error: Must define a valid path for the world map file.");
			Shutdown.now();
		}
		return prop;
	}

	private Config conf;

	private AgentInfo()
	{
		String filePath = System.getProperty("config-path");
		if (StringUtils.isBlank(filePath))
		{
			log.error("Fatal error: No configuration file defined. Please set the enviroment variable 'config-path' to valid path for the configuration file");
			Shutdown.now();
		}
		try
		{
			JAXBContext jc = JAXBContext.newInstance("pt.com.gcs.conf");
			Unmarshaller u = jc.createUnmarshaller();

			conf = (Config) u.unmarshal(new File(filePath));
		}
		catch (JAXBException e)
		{
			log.error("Fatal error: {}", e.getMessage());
			Shutdown.now();
		}
	}

	public static int getBrokerHttpPort()
	{
		int iprop = instance.conf.getNet().getBrokerHttpPort();
		return iprop;
	}
	
	public static int getBrokerPort()
	{
		int iprop = instance.conf.getNet().getBrokerPort();
		return iprop;
	}

}
