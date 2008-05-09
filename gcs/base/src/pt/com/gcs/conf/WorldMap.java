package pt.com.gcs.conf;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import org.caudexorigo.Shutdown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import pt.com.gcs.net.Peer;

public class WorldMap
{
	private static final Logger log = LoggerFactory.getLogger(WorldMap.class);

	private final List<Peer> peerList = new ArrayList<Peer>();
	
	private static final WorldMap instance = new WorldMap();
	
	private AtomicLong last_modified = new AtomicLong(0L);


	private WorldMap()
	{
		populateWorldMap();
	}
	
	private synchronized void populateWorldMap()
	{
		String selfName = GcsInfo.getAgentName();
		String selfHost = GcsInfo.getAgentHost();
		int selfPort = GcsInfo.getAgentPort();

		String worldMapPath = GcsInfo.getWorldMapPath();
		Source schemaLocation = new StreamSource(WorldMap.class.getResourceAsStream("/pt/com/gcs/etc/world_map.xsd"));
		File xmlFile = new File(worldMapPath);

		XsdValidationResult result = SchemaValidator.validate(schemaLocation, xmlFile);

		if (!result.isValid())
		{
			log.error("Invalid world map, aborting startup.");
			log.error(result.getMessage());
			Shutdown.now();
		}

		Document doc = parseXmlFile(worldMapPath, false);

		// Get a list of all elements in the document

		int npeers = doc.getElementsByTagName("peer").getLength();
		String[] names = extractPeerInfo(doc, "name");
		String[] hosts = extractPeerInfo(doc, "ip");
		String[] ports = extractPeerInfo(doc, "port");
		
		//System.out.println("_selfName: " + _selfName);
		
		boolean isSelfPeerInWorldMap = false;
		
		peerList.clear();

		for (int i = 0; i < npeers; i++)
		{
			if (selfName.equalsIgnoreCase(names[i]))
			{
				if (selfHost.equalsIgnoreCase(hosts[i]))
				{
					if (selfPort == Integer.parseInt(ports[i]))
					{
						isSelfPeerInWorldMap =  true;
					}
				}
			}
			else
			{
				//System.out.println("names[i]: " + names[i]);
				peerList.add(new Peer(names[i], hosts[i], Integer.parseInt(ports[i])));
			}
		}
		
		if (!isSelfPeerInWorldMap)
		{
			System.err.println("This peer it's not in the world map.");
			Shutdown.now();
		}
	}

	private Document parseXmlFile(String filename, boolean validating)
	{
		try
		{
			// Create a builder factory
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			factory.setValidating(validating);
			File xmlFile = new File(filename);
			last_modified.set(xmlFile.lastModified());

			// Create the builder and parse the file
			Document doc = factory.newDocumentBuilder().parse(xmlFile);
			return doc;
		}
		catch (Throwable t)
		{
			throw new RuntimeException(t);
		}
	}

	private String[] extractPeerInfo(Document doc, String tag)
	{
		NodeList nList = doc.getElementsByTagName(tag);
		String[] value = new String[nList.getLength()];

		for (int i = 0; i < nList.getLength(); i++)
		{
			Element name = (Element) nList.item(i);
			value[i] = name.getTextContent();
		}

		return value;
	}

	public static List<Peer> getPeerList()
	{
		return Collections.unmodifiableList(instance.peerList);
	}
	
	public static long lastModified()
	{
		return instance.last_modified.get();
	}

}
