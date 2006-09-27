package pt.com.manta;

import java.io.File;
import java.io.FileReader;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.mr.MantaAgentConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import pt.com.text.StringUtils;

public class WorldMapReloader
{
	private static Logger log = LoggerFactory.getLogger(WorldMapReloader.class);

	private static WorldMapReloader instance = new WorldMapReloader();

	private final ScheduledThreadPoolExecutor _fileChangeChecker = new ScheduledThreadPoolExecutor(2);

	private File _worldMapFile;

	private long _lastModified;

	private static long INITIAL_DELAY = 10000L; // 10 segundos

	private static long CHECK_DELAY = 60000L; // 60 segundos

	private static long RELOAD_DELAY = 900000L; // 900 segundos

	private AtomicBoolean _isRunning = new AtomicBoolean(false);

	private WorldMapReloader()
	{
		_worldMapFile = findWorldMapFile();
		if (_worldMapFile != null)
		{
			_lastModified = _worldMapFile.lastModified();
		}
	}

	public static WorldMapReloader getInstance()
	{
		return instance;
	}

	final Runnable reloader = new Runnable()
	{
		public void run()
		{
			Management.reloadWorld();
		}
	};

	final Runnable monitor = new Runnable()
	{
		public void run()
		{
			log.debug("Checking for changes in the world map.");
			synchronized (this)
			{
				if ((_lastModified != _worldMapFile.lastModified()) && (_fileChangeChecker.getQueue().size() <= 1))
				{
					log.debug("World map definition file changed");
					_lastModified = _worldMapFile.lastModified();
					long now = System.currentTimeMillis();
					long when = (_lastModified + RELOAD_DELAY) - now;

					if (when > 0)
					{
						log.debug("World map definition reload in :" + when + "ms");
						final ScheduledFuture<?> reloaderHandle = _fileChangeChecker.schedule(reloader, when, TimeUnit.MILLISECONDS);
					}
				}
			}
		}
	};

	public void checkFileModifications()
	{
		if (!_isRunning.get())
		{
			if (!_fileChangeChecker.isShutdown())
			{
				final ScheduledFuture<?> monitorHandle = _fileChangeChecker.scheduleAtFixedRate(monitor, INITIAL_DELAY, CHECK_DELAY, TimeUnit.MILLISECONDS);
				_isRunning.set(true);
			}
		}
	}

	private File findWorldMapFile()
	{
		try
		{
			File configFile = new File(System.getProperty(MantaAgentConstants.MANTA_CONFIG));

			InputSource configSource = new org.xml.sax.InputSource(new FileReader(configFile));
			if (configSource.getCharacterStream() == null)
			{
				throw new RuntimeException("Mantaray configuration file not found!");
			}

			XPathFactory factory = XPathFactory.newInstance();
			XPath xpath = factory.newXPath();
			XPathExpression expression = xpath.compile("//file_ref/text()");
			String result = expression.evaluate(configSource);
			log.info("World Map in use is: " + result);

			if (StringUtils.isNotBlank(result))
			{
				File wmf = new File(result);
				if (wmf.exists())
				{
					return wmf;
				}

				else
				{
					abort();
					return null;
				}
			}
			else
			{
				abort();
				return null;
			}
		}
		catch (Exception e)
		{
			log.error(e.getMessage(), e);
			abort();
			return null;
		}
	}

	private void abort()
	{
		_fileChangeChecker.shutdown();
		log.warn("Mantaray configuration file not found! Automatic refreshing of the word map is disabled.");
	}
}