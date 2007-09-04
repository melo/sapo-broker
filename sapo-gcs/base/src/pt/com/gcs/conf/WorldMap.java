package pt.com.gcs.conf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import pt.com.gcs.Shutdown;
import pt.com.gcs.net.Peer;

public class WorldMap
{
	private static Logger log = LoggerFactory.getLogger(WorldMap.class);

	private static final List<Peer> _peerList = new ArrayList<Peer>();


	public WorldMap()
	{
		String selfName = AgentInfo.getAgentName();
		String selfHost = AgentInfo.getAgentHost();
		int selfPort = AgentInfo.getAgentPort();

		String worldMapPath = AgentInfo.getWorldMapPath();
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
				_peerList.add(new Peer(names[i], hosts[i], Integer.parseInt(ports[i])));
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

			// Create the builder and parse the file
			Document doc = factory.newDocumentBuilder().parse(new File(filename));
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

	public List<Peer> getPeerList()
	{
		return _peerList;
	}

}
