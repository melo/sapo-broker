package pt.com.manta;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.mr.MantaAgent;
import org.mr.MantaAgentConstants;
import org.mr.core.configuration.ConfigManager;
import org.mr.kernel.world.WorldModeler;
import org.mr.kernel.world.WorldModelerLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.AcknowledgeMode;
import pt.com.broker.BrokerConsumer;
import pt.com.broker.MQ;

public class Management implements MessageListener
{
	private static Logger log = LoggerFactory.getLogger(Management.class);
	private MessageConsumer _mng_consumer;
	
	public Management()
	{
		try
		{
			_mng_consumer = BrokerConsumer.getInstance().getMessageConsumer(AcknowledgeMode.AUTO, "TOPIC", MQ.MANAGEMENT_TOPIC);
			_mng_consumer.setMessageListener(this);
		}
		catch (Exception je)
		{
			log.error("Oopps!", je);
			System.exit(-1);
		}
	}
	
	public void onMessage(Message message)
	{
		//TODO: implement mangement events to control the entire mech, for now this is only a stub		
	}

	public static synchronized void reloadWorld()
	{
		log.info("Trying to reload the world map");
		String mantaConfigurationFile = System.getProperty(MantaAgentConstants.MANTA_CONFIG);
		ConfigManager configManager;
		try
		{
			configManager = new ConfigManager(mantaConfigurationFile);
			MantaAgent.getInstance().getSingletonRepository().setConfigManager(configManager);

			WorldModeler world = MantaAgent.getInstance().getSingletonRepository().getWorldModeler();
			// load loads all the other peers and services
			WorldModelerLoader.load(world);
			log.info("Successfully reload of the world map");
		}
		catch (Exception e)
		{
			throw new RuntimeException("reloadWorldMap error", e);
		}
	}
}
