package pt.com.gcs.messaging;

import java.io.File;

import org.caudexorigo.Shutdown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.conf.AgentInfo;
import pt.com.gcs.conf.WorldMap;

public class WorldMapMonitor implements Runnable
{

	private static final Logger log = LoggerFactory.getLogger(WorldMapMonitor.class);

	@Override
	public void run()
	{
		log.debug("Checking world map file for modifications.");
		
		String worldMapPath = AgentInfo.getWorldMapPath();
		File worldMapFile = new File(worldMapPath);
		
		if (worldMapFile.lastModified()!=WorldMap.lastModified())
		{
			// Temporary "solution". The agent will be restarted by external monitoring services (e.g.: supervise)
			
			log.warn("WorlMap has changed. The agent will restart");
			Shutdown.now();
		}

	}

}
