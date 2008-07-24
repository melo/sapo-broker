package pt.com.gcs.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.gcs.conf.WorldMap;

public class WorldMapMonitor implements Runnable
{

	private static final Logger log = LoggerFactory.getLogger(WorldMapMonitor.class);

	@Override
	public void run()
	{
		log.debug("Checking world map file for modifications.");

		if (WorldMap.reload())
		{
			Gcs.reloadWorldMap();
		}
	}

}
