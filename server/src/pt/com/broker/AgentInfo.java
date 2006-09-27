package pt.com.broker;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.mr.MantaAgent;
import org.mr.SingletonRepository;
import org.mr.core.configuration.ConfigManager;
import org.mr.core.log.StartupLogger;
import org.mr.core.net.LocalTransport;
import org.mr.core.net.TransportInfo;
import org.mr.kernel.world.WorldModeler;
import org.mr.plugins.discovery.ADControlSender;

public class AgentInfo
{

	public static final String AGENT_NAME = MantaAgent.getInstance().getAgentName();
	
	private String agentName;

	private String agentDomain;

	private String wblinkPassword;

	private String remove_surl;

	private String add_surl;

	private boolean isWblinkEnabled;

	private static final AgentInfo instance = new AgentInfo();

	private AgentInfo()
	{
		try
		{
			ConfigManager configManager = MantaAgent.getInstance().getSingletonRepository().getConfigManager();

			isWblinkEnabled = Boolean.parseBoolean(configManager.getStringProperty("wblink.enabled"));

			if (isWblinkEnabled)
			{
				WorldModeler world = MantaAgent.getInstance().getSingletonRepository().getWorldModeler();

				agentName = generateRealName(configManager.getStringProperty("my-peer.name", "m%ip%%port%"));
				agentDomain = configManager.getStringProperty("my-peer.domain");
				wblinkPassword = configManager.getStringProperty("wblink.password");

				SingletonRepository singletonRepository = new SingletonRepository();
				singletonRepository.setConfigManager(configManager);

				String mwbHost = "";

				Set irsTransports = world.getAgentTransportInfo(agentDomain, "mwb");
				for (Iterator iter = irsTransports.iterator(); iter.hasNext();)
				{
					TransportInfo tinfo = (TransportInfo) iter.next();
					mwbHost = ((InetSocketAddress) tinfo.getSocketAddress()).getAddress().getHostAddress();
				}

				remove_surl = "http://" + mwbHost + ":7070/InvokeAction//MWB%3Amwb%3DMWB+management/action=removeAgent?action=removeAgent&Agent+name%2Bjava.lang.String=" + agentName + "&Domain+name%2Bjava.lang.String=" + agentDomain;

				add_surl = "http://" + mwbHost + ":7070/InvokeAction//MWB%3Amwb%3DMWB+management/action=addAgent?action=addAgent&Agent+name%2Bjava.lang.String=" + agentName + "&Domain+name%2Bjava.lang.String=" + agentDomain + "&Password%2Bjava.lang.String=" + wblinkPassword;
			}

		}
		catch (Exception e)
		{
			System.err.println(e.getMessage());
			System.err.println("\n");
			System.err.println("Unable to query the connection information for Wan Bridge");
			System.err.println("\n");

			System.exit(-1);
		}

	}

	public static void publishToMWB()
	{
		try
		{
			if (instance.isWblinkEnabled)
			{
				makeCall(instance.remove_surl);
				makeCall(instance.add_surl);
			}
		}
		catch (Exception e)
		{

			System.err.println(e.getMessage());
			System.err.println("\n");
			System.err.println("Unable to publish agent info on Wan Bridge");
			System.err.println("\n");

			System.exit(-1);
		}
	}

	public static void removeFromMWB()
	{
		try
		{
			if (instance.isWblinkEnabled)
			{
				makeCall(instance.remove_surl);
			}
		}
		catch (Exception e)
		{
			// since we are removing the agent we will let die silently
		}
	}

	private static void makeCall(String surl) throws Exception
	{
		System.out.println("\n CALL:\n" + surl + "\n");
		URL url = new URL(surl);

		BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));


		while (in.readLine() != null)
		{
			// NOP
		}
		in.close();
	}

	private static String generateRealName(String myAgentName)
	{
		String result = null;
		if (myAgentName.indexOf("%") == -1)
		{
			result = myAgentName;
		}
		else
		{
			Collection transports = MantaAgent.getInstance().getSingletonRepository().getNetworkManager().getLocalTransports();
			Iterator i = transports.iterator();
			if (i.hasNext())
			{
				LocalTransport lt = (LocalTransport) i.next();
				String port = String.valueOf(lt.getInfo().getPort());
				String ip = lt.getInfo().getIp().getHostAddress();
				if (ip.equals("0.0.0.0"))
				{
					ip = ADControlSender.getValidLocalAddress();
				}
				myAgentName = myAgentName.replaceAll("%port%", port);
				myAgentName = myAgentName.replaceAll("%ip%", ip);
				result = myAgentName;
				if ("127.0.0.1".equals(ip))
				{
					StartupLogger.log.fatal("MantaRay layer's name was resolved to '" + ip + "'. MantaRay will not work properly.", "WorldModelerLoader");
				}
			}
			else
			{
				// System.out.println("FATAL Could not find transport
				// information for this layer");
				StartupLogger.log.fatal("Could not find transport information for this layer.", "WorldModelerLoader");
				result = "UNKNOWN";
			}

		}

		return result;
	}

}
