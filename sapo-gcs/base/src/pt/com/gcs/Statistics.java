package pt.com.gcs;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.caudexorigo.ds.Cache;
import org.caudexorigo.ds.CacheFiller;
import pt.com.gcs.conf.AgentInfo;
import pt.com.gcs.messaging.Message;
import pt.com.gcs.tasks.GcsExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Statistics
{
	private static Logger log = LoggerFactory.getLogger(Statistics.class);

	private static final Statistics instance = new Statistics();

	private final Counter produced = new Counter();

	private final Counter received = new Counter();

	private final Counter dropped = new Counter();

	private final Cache<String, Counter> producedByDestination = new Cache<String, Counter>();

	private final Cache<String, Counter> receivedByDestination = new Cache<String, Counter>();

	private final Cache<String, Counter> droppedByApp = new Cache<String, Counter>();

	private final String statsTopicService;

	private final boolean collectStats;

	private final boolean sendUpdates;

	private final long updateInterval;

	static final CacheFiller<String, Counter> ccfill = new CacheFiller<String, Counter>()
	{
		public Counter populate(String key)
		{
			return new Counter();
		}
	};

	private Statistics()
	{
		statsTopicService = AgentInfo.getStatisticsTopic();
		collectStats = true;
		sendUpdates = true;
		updateInterval = 30L;

		log.debug("Stats, statsTopicService: " + statsTopicService);
		log.debug("Stats, collectStats: " + collectStats);
		log.debug("Stats, sendUpdates: " + sendUpdates);
		log.debug("Stats, updateInterval: " + updateInterval);
	}

	final Runnable resetCounter = new Runnable()
	{
		public void run()
		{
			if (sendUpdates)
			{
				publishStats();
			}
			resetParcialCounters();
		}

		private String gatherStats()
		{

			Set<String> skeys = new HashSet<String>();
			skeys.addAll(producedByDestination.keys());
			skeys.addAll(receivedByDestination.keys());
			skeys.addAll(droppedByApp.keys());

			long now = System.currentTimeMillis();
			StringBuilder buf = new StringBuilder();

			buf.append("<Stats xmlns='http://uri.sapo.pt/schemas/broker/stats.xsd'>");
			buf.append("<Node>" + AgentInfo.getAgentName() + "</Node>");
			buf.append("<Version>" + AgentInfo.AGENT_VERSION + "</Version>");
			buf.append("<ConfigVersion>" + AgentInfo.getConfigVersion() + "</ConfigVersion>");
			buf.append("<Timestamp>" + now + "</Timestamp>");
			buf.append("<Interval>" + 120000 + "</Interval>");
			buf.append("<InternalCounters>");

			buf.append("<Counter>");
			buf.append("<Name>incoming_accepted</Name>");
			buf.append("<Total>" + received.getTotal() + "</Total>");
			buf.append("<SinceLastCheck>" + received.getParcial() + "</SinceLastCheck>");
			buf.append("</Counter>");

			buf.append("<Counter>");
			buf.append("<Name>published</Name>");
			buf.append("<Total>" + produced.getTotal() + "</Total>");
			buf.append("<SinceLastCheck>" + produced.getParcial() + "</SinceLastCheck>");
			buf.append("</Counter>");

			buf.append("<Counter>");
			buf.append("<Name>dropped_messages</Name>");
			buf.append("<Total>" + dropped.getTotal() + "</Total>");
			buf.append("<SinceLastCheck>" + dropped.getParcial() + "</SinceLastCheck>");
			buf.append("</Counter>");

			buf.append("</InternalCounters>");
			buf.append("<Applications>");

			try
			{
				for (String src : skeys)
				{
					Counter cnp = producedByDestination.get(src, ccfill);
					Counter cnr = receivedByDestination.get(src, ccfill);
					Counter cnd = droppedByApp.get(src, ccfill);

					buf.append("<Application name='" + src + "'>");

					buf.append("<Counters>");

					buf.append("<Counter>");
					buf.append("<Name>incoming_accepted</Name>");
					buf.append("<Total>" + cnr.getTotal() + "</Total>");
					buf.append("<SinceLastCheck>" + cnr.getParcial() + "</SinceLastCheck>");
					buf.append("</Counter>");

					buf.append("<Counter>");
					buf.append("<Name>published</Name>");
					buf.append("<Total>" + cnp.getTotal() + "</Total>");
					buf.append("<SinceLastCheck>" + cnp.getParcial() + "</SinceLastCheck>");
					buf.append("</Counter>");

					buf.append("<Counter>");
					buf.append("<Name>dropped_messages</Name>");
					buf.append("<Total>" + cnd.getTotal() + "</Total>");
					buf.append("<SinceLastCheck>" + cnd.getParcial() + "</SinceLastCheck>");
					buf.append("</Counter>");

					buf.append("</Counters>");

					buf.append("</Application>");
				}
				buf.append("</Applications>");

				buf.append("</Stats>");

			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
			}

			return buf.toString();
		}

		private void publishStats()
		{
			String statsXml = gatherStats();
			String source = "broker://agent/" + AgentInfo.getAgentName() + "/stats";
			Message stats_msg = new Message(AgentInfo.getStatisticsTopic(), statsXml);
			stats_msg.setSourceApp(source);

			Gcs.publish(stats_msg);

		}

		private void resetParcialCounters()
		{
			produced.reset();
			received.reset();
			dropped.reset();

			try
			{
				for (Counter cn : producedByDestination.values())
				{
					cn.reset();
				}

				for (Counter cn : receivedByDestination.values())
				{
					cn.reset();
				}

				for (Counter cn : droppedByApp.values())
				{
					cn.reset();
				}
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
			}
		}

	};

	public static void init()
	{
		if (instance.collectStats)
		{
			GcsExecutor.scheduleAtFixedRate(instance.resetCounter, instance.updateInterval, instance.updateInterval, TimeUnit.SECONDS);
		}
	}

	public static void messageProduced(String source)
	{
		if (instance.collectStats)
		{
			instance.produced.increment();

			try
			{
				instance.producedByDestination.get(source, ccfill).increment();
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
			}
		}
	}

	public static void messageReceived(String source)
	{
		if (instance.collectStats)
		{
			instance.received.increment();

			try
			{
				instance.receivedByDestination.get(source, ccfill).increment();
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
			}
		}
	}

	public static void messageDropped(String source)
	{
		if (instance.collectStats)
		{
			instance.dropped.increment();
			try
			{
				instance.droppedByApp.get(source, ccfill).increment();
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
			}
		}
	}
}
