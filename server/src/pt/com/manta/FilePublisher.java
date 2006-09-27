package pt.com.manta;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.util.concurrent.TimeUnit;

import org.mr.MantaAgent;
import org.mr.core.configuration.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.BrokerExecutor;
import pt.com.broker.BrokerProducer;
import pt.com.broker.MQ;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapSerializer;

public class FilePublisher
{
	private static Logger log = LoggerFactory.getLogger(FilePublisher.class);

	private static FilePublisher instance = new FilePublisher();

	private static final long INITIAL_DELAY = 10L; // 10 segundos

	private static final long CHECK_DELAY = 60L; // 60 segundos

	private final String dir;

	private final boolean isEnabled;

	private final long check_interval;

	private File dropBoxDir;

	private FilePublisher()
	{
		ConfigManager config = MantaAgent.getInstance().getSingletonRepository().getConfigManager();
		isEnabled = config.getBooleanProperty("sapo:dropbox.enabled", false);
		dir = config.getStringProperty("sapo:dropbox.dir", ".");
		check_interval = config.getLongProperty("sapo:dropbox.check_interval", CHECK_DELAY);

		log.debug("DropBox Monitor, dir: " + dir);
		log.debug("DropBox Monitor, enabled: " + isEnabled);
		log.debug("DropBox Monitor, check interval: " + check_interval);

	}

	final FileFilter fileFilter = new FileFilter()
	{
		public boolean accept(File file)
		{
			return file.getName().endsWith(".good");
		}
	};

	final Runnable publisher = new Runnable()
	{
		public void run()
		{
			log.debug("Checking for files in the DropBox.");
			try
			{
				File[] files = dropBoxDir.listFiles(fileFilter);

				if (files.length > 0)
				{
					if (log.isDebugEnabled())
					{
						log.debug("Will try to publish " + files.length + " file(s).");
					}

					for (File msgf : files)
					{
						FileInputStream fis = new FileInputStream(msgf);
						SoapEnvelope soap = null;
						boolean isFileValid = false;
						try
						{
							soap = SoapSerializer.FromXml(fis);
							isFileValid = true;
						}
						catch (Throwable e)
						{
							log.error("Erro processing file \"" + msgf.getAbsolutePath() + "\". Error message: " + ErrorHandler.findRootCause(e).getMessage());
						}

						if (isFileValid)
						{
							BrokerProducer.getInstance().publishMessage(soap.body.publish, MQ.requestSource(soap));
							fis.close();
							msgf.delete();
						}
						else
						{
							fis.close();
							File badFile = new File(msgf.getAbsolutePath() + ".bad");
							msgf.renameTo(badFile);
						}

					}
				}
				else
				{
					log.debug("No files to publish.");
				}
			}
			catch (Throwable e)
			{
				log.error(e.getMessage(), e);
			}

		}
	};

	public static void init()
	{
		if (instance.isEnabled)
		{
			instance.dropBoxDir = new File(instance.dir);
			if (instance.dropBoxDir.isDirectory())
			{
				BrokerExecutor.scheduleWithFixedDelay(instance.publisher, INITIAL_DELAY, instance.check_interval, TimeUnit.SECONDS);
				log.info("Drop box functionality enabled");
			}
			else
			{
				abort();
			}
		}
		else
		{
			abort();
		}
	}

	private static void abort()
	{
		log.info("Drop box functionality disabled");
	}
}