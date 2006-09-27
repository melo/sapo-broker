/*
 * Copyright 2002 by
 * <a href="http://www.coridan.com">Coridan</a>
 * <a href="mailto: support@coridan.com ">support@coridan.com</a>
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is "MantaRay" (TM).
 *
 * The Initial Developer of the Original Code is Amir Shevat .
 * Portions created by the Initial Developer are Copyright (C) 2006
 * Coridan Inc.  All Rights Reserved.
 *
 * Contributor(s): all the names of the contributors are added in the source
 * code where applicable.
 *
 * Alternatively, the contents of this file may be used under the terms of the
 * LGPL license (the "GNU LESSER GENERAL PUBLIC LICENSE"), in which case the
 * provisions of LGPL are applicable instead of those above.  If you wish to
 * allow use of your version of this file only under the terms of the LGPL
 * License and not to allow others to use your version of this file under
 * the MPL, indicate your decision by deleting the provisions above and
 * replace them with the notice and other provisions required by the LGPL.
 * If you do not delete the provisions above, a recipient may use your version
 * of this file under either the MPL or the GNU LESSER GENERAL PUBLIC LICENSE.

 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the MPL as stated above or under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * either version 2.1 of the License, or any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
 * License for more details.
 */

package org.mr;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mr.api.jms.MantaConnection;
import org.mr.api.rmi.MantaRMIServer;
import org.mr.core.cmc.MantaJMXManagement;
import org.mr.core.configuration.ConfigManager;
import org.mr.core.groups.MutlicastGroupManager;
import org.mr.core.log.StartupLogger;
import org.mr.core.net.MantaAddress;
import org.mr.core.net.NetworkManager;
import org.mr.core.persistent.PersistentConst;
import org.mr.core.protocol.MantaBusMessage;
import org.mr.core.protocol.MantaBusMessageConsts;
import org.mr.core.protocol.MantaBusMessageUtil;
import org.mr.core.protocol.MessageTransformer;
import org.mr.core.stats.StatManager;
import org.mr.core.util.SystemTime;
import org.mr.core.util.byteable.ByteBufferPool;
import org.mr.core.util.byteable.ByteableRegistry;
import org.mr.indexing.WBManager;
import org.mr.kernel.BlockingMessageListener;
import org.mr.kernel.DelayedMessageSender;
import org.mr.kernel.IncomingClientMessageRouter;
import org.mr.kernel.IncomingMessageManager;
import org.mr.kernel.PluginManager;
import org.mr.kernel.UniqueIDGenerator;
import org.mr.kernel.control.ControlSignal;
import org.mr.kernel.control.ControlSignalMessageConsumer;
import org.mr.kernel.delivery.DeliveryAckListener;
import org.mr.kernel.delivery.DeliveryAckNotifier;
import org.mr.kernel.delivery.PostOffice;
import org.mr.kernel.security.SecurityInitializer;
import org.mr.kernel.services.MantaService;
import org.mr.kernel.services.SelectorsManager;
import org.mr.kernel.services.ServiceActor;
import org.mr.kernel.services.ServiceActorControlCenter;
import org.mr.kernel.services.ServiceConsumer;
import org.mr.kernel.services.ServiceProducer;
import org.mr.kernel.services.ServiceRecallShutdownHook;
import org.mr.kernel.services.queues.AbstractQueueService;
import org.mr.kernel.services.queues.QueueMaster;
import org.mr.kernel.services.queues.QueueServiceFactory;
import org.mr.kernel.services.queues.RemoteQueueEnumeration;
import org.mr.kernel.services.queues.VirtualQueuesManager;
import org.mr.kernel.services.topics.VirtualTopicManager;
import org.mr.kernel.world.WorldModeler;
import org.mr.kernel.world.WorldModelerLoader;
import org.w3c.dom.Element;

import pt.com.manta.Start;

/**
 * This class represents the point of entry to the manta layer. <br>
 * It holds and expose reference to all the classes needed for working with the
 * manta layer. <br>
 * The class is a singleton and thread safe for use. <br>
 * <b>Note </b> that system property 'mantaConfig' must be set to the path where
 * the manta configuration file is. To do this use the
 * System.setProperty("mantaConfig", [i.e c:/manta/config.xml]) or by the -D
 * parameter in the java command line see examples for proper use.
 * 
 * @author Amir Shevat
 * @version 1.0
 * @see MantaAgentConstants
 * @since Dec 29, 2003
 */
public class MantaAgent
{

	private SingletonRepository singletonRepository = null;

	private DynamicRepository dynamicRepository;

	private static MantaAgent instance = null;

	private static Log log;

	private String mantaConfigurationFile;

	/**
	 * true if manta agent has been instanced else false
	 */
	public static boolean started = false;

	private String myName;

	/**
	 * Added by Shirley Sasson - 07/06/06 Holds the configuration as a DOM
	 * element. Will be not null onlt if configuration was set from an outside
	 * application using MantaConnectionFactory.setConfiguration method.
	 */
	private static Element configurationElement;

	/**
	 * This is the singleton entry point calling this method for the <u>first
	 * </u> time inits the agent and should be longer the the rest of the method
	 * called.
	 * 
	 * @return the singleton instance of the manta layer API
	 */
	public static MantaAgent getInstance()
	{
		if (instance == null)
		{
			synchronized (MantaAgent.class)
			{
				if (instance == null)
					instance = new MantaAgent();
			}
		}

		return instance;

	}// getInstance

	/**
	 * Inits all the manta layer. Constractor for the MantaAgent.
	 */
	private MantaAgent()
	{
		StartupLogger.log.startStore();
		try
		{
			// Added by Shirley Sasson - 07/06/06
			// If configuration was set by an outside application using
			// MantaConnectionFactory.setConfiguration method,
			// don't look for a VM paramter with the name mantaConfig.
			if (configurationElement == null)
			{
				mantaConfigurationFile = System.getProperty(MantaAgentConstants.MANTA_CONFIG);
				if (mantaConfigurationFile == null)
				{
					StartupLogger.log.fatal("MantaRay configuration not set! - Please set the system property 'mantaConfig' to the location of MantaRay configuration file, or use setConfiguration method to supply a DOM element for configuration.", "MantaAgent");
					StartupLogger.log.fatal("In order for manta to work properly either of the two configurations need to be set.", "MantaAgent");
					mantaConfigurationFile = "./default_config.xml";
				}
				StartupLogger.log.info("property 'mantaConfig'=" + mantaConfigurationFile, "MantaAgent");

				// validate that the params exist end notify about it
				FileOrFolderExists(mantaConfigurationFile);
			}
		}
		catch (Throwable t)
		{
			t.printStackTrace();
		}// catch

	}// MantaAgent

	/**
	 * starts the manta layer - mainly inits the singletons this method will run
	 * only once after the first run it will allways return true
	 * 
	 * @return false if this is the first time this method was invoked and an
	 *         error has happend, else true
	 */
	public synchronized boolean init()
	{
		if (started)
			return true;
		try
		{
			// Added by Shirley Sasson - 07/06/06
			// Create a new ConfigManager object according to the configuration
			// supplied (file or DOM element).
			ConfigManager configManager = null;
			if (configurationElement != null)
			{
				configManager = new ConfigManager(configurationElement);
				// set reference to null cause not needed anymore and should be
				// garbage collected
				configurationElement = null;
			}
			else
				configManager = new ConfigManager(mantaConfigurationFile);

			this.singletonRepository = new SingletonRepository();
			singletonRepository.setConfigManager(configManager);

			singletonRepository.setVirtualTopicManager(new VirtualTopicManager());
			SystemTime.init();

			StatManager statManager = new StatManager();
			singletonRepository.setStatManager(statManager);

			// initialize the authentication and authorization implementations
			SecurityInitializer.initialize();

			// init network
			NetworkManager networkmanager = new NetworkManager(WorldModeler.getInstance(), statManager);
			networkmanager.createServerSockets();
			singletonRepository.setNetworkManager(networkmanager);

			String defaultPersistentDir = "./persistent";
			String persistentDir = configManager.getStringProperty("persistency.file.persistent_folder", defaultPersistentDir);
			if (persistentDir.trim().length() == 0)
				persistentDir = defaultPersistentDir;
			PersistentConst.setPersistentDir(persistentDir);
			int numOfSmallBuffersInPool = configManager.getIntProperty("small_buffer_pool_size", 100);
			int numOfMediumBuffersInPool = configManager.getIntProperty("medium_buffer_pool_size", 50);
			int numOfLargeBuffersInPool = configManager.getIntProperty("big_buffer_pool_size", 10);
			if (configManager.getBooleanProperty("LazyMessageParsing", false))
			{
				MantaBusMessage.setLazyParsing();
			}
			PersistentConst.setPersistentByteBufferPool(new ByteBufferPool(numOfSmallBuffersInPool, numOfMediumBuffersInPool, numOfLargeBuffersInPool));

			MessageTransformer transformer = new MessageTransformer();

			// init ByteableRegistry
			ByteableRegistry.init();

			// start jmx addaptors
			// load jmx - added by lital kasif
			singletonRepository.setDelayedMessageSender(new DelayedMessageSender());
			singletonRepository.setDeliveryAckNotifier(new DeliveryAckNotifier());

			singletonRepository.setPostOffice(new PostOffice(WorldModeler.getInstance()));

			singletonRepository.setVirtualQueuesManager(new VirtualQueuesManager());

			// singletonRepository.setVirtualTopicManager(new
			// VirtualTopicManager());

			singletonRepository.setWorldModeler(WorldModeler.getInstance());
			singletonRepository.setControlSignalMessageConsumer(new ControlSignalMessageConsumer());
			singletonRepository.setIncomingMessageManager(new IncomingMessageManager());
			singletonRepository.setIncomingClientMessageRouter(new IncomingClientMessageRouter());
			singletonRepository.setSelectorsManager(new SelectorsManager());
			singletonRepository.setGroupsManager(new MutlicastGroupManager());
			// singletonRepository.setControlSignalMessageSender(new
			// ControlSignalMessageSender());
			singletonRepository.setServiceActorControlCenter(new ServiceActorControlCenter());

			// init the world
			WorldModeler world = singletonRepository.getWorldModeler();
			WorldModelerLoader.init(world);
			myName = world.getMyAgentName();

			dynamicRepository = new DynamicRepository();
			dynamicRepository.init();
			QueueServiceFactory queueServiceFactory = (QueueServiceFactory) dynamicRepository.getImplementation("queueFactory");
			singletonRepository.setQueueServiceFactory(queueServiceFactory);

//			 PATCH: we don't need the JMX support and I want to get read of the jmx *.jar files
//          MantaJMXManagement jmxManager =MantaJMXManagement.getInstance();
//			singletonRepository.setMantaJMXManagement(jmxManager);
//          jmxManager.startConnections();
//          singletonRepository.setMantaJMXManagement(jmxManager);
//          jmxManager.startConnections();

			// go on with the loader
			// load loads all the other peers and services
			WorldModelerLoader.load(world);

			singletonRepository.setWBManager(new WBManager());

			// The Plugin manager is used to initialize plugins.
			singletonRepository.setPluginManager(new PluginManager());

			log = LogFactory.getLog("MantaAgent");
			// rmi API
			//MantaRMIServer.init();

			// registers all the JMX managed objects
			MantaJMXManagement.registerJMXBeans();
			Runtime.getRuntime().addShutdownHook(new ServiceRecallShutdownHook());

			networkmanager.startServers();

			// nimo, 15JUN2005 - let manta know the world before allowing to
			// publish.
			Thread.sleep(configManager.getIntProperty("plug-ins.auto-discovery.init_discovery_delay", 1000));

			log = LogFactory.getLog("MantaAgent");

			if (StartupLogger.log.hasFatals())
			{
				System.out.println("*** MANTARAY LOADED WITH FATAL ERRORS!!! ***");
				log.fatal("MANTARAY LOADED WITH FATAL ERRORS AND WILL NOT BE ABLE TO FUNCTION PROPERLY!!!");
				log.fatal("PLEASE REFER TO THE MANTARAY LOG FOR FURTHER INFORMATION.");
				return false;
			}
			else if (StartupLogger.log.hasErrors())
			{
				System.out.println("*** MANTARAY LOADED WITH ERRORS!! ***");
				log.error("MANTARAY LOADED WITH ERRORS!! PROBABLY MANTARAY WILL NOT BE ABLE TO FUNCTION PROPERLY!!");
				log.error("PLEASE REFER TO THE MANTARAY LOG FOR FURTHER INFORMATION.");
				return false;
			}
			else if (StartupLogger.log.hasWarnings())
			{
				System.out.println("*** MANTARAY LOADED WITH WARNINGS! ***");
				log.warn("MANTARAY LOADED WITH WARNINGS! MANTARAY MIGHT NOT BE ABLE TO FUNCTION PROPERLY!");
				log.warn("PLEASE REFER TO THE MANTARAY LOG FOR FURTHER INFORMATION.");
				return false;
			}
			else
			{
				System.out.println(Version.version + " initialization completed.");
				log.info(Version.version + " initialization completed.");
				log.info("MANTARAY LOADED (Don't Panic).");
			}
		}
		catch (Exception e1)
		{
			e1.printStackTrace();
			started = true;
			return false;
		}
		started = true;
		return true;
	}

	/**
	 * Returns true is file or folder exists else false
	 * 
	 * @param fileOrFolderName
	 *            the name of the file or the folder
	 * @return true is file or folder exists else false
	 */
	private boolean FileOrFolderExists(String fileOrFolderName)
	{
		File f = new File(fileOrFolderName);
		if (!f.exists())
		{
			// System.out.println(fileOrFolderName + " NOT found in your file
			// system!");
			StartupLogger.log.info(fileOrFolderName + " NOT found in your file system!", "MantaAgent");
			return false;
		}
		else
		{
			// System.out.println(fileOrFolderName + " found.");
			StartupLogger.log.info(fileOrFolderName + " found.", "MantaAgent");
			return true;
		}

	}

	// //////////////////////////////////////
	// / QUEUES
	// //////////////////////////////////////

	/**
	 * Enqueue a manta bus message to a queue service , this method will not
	 * return untill the message is in the queue coordinator side <b>Note: you
	 * MUST advertise yourself as queue producer before invoking this method.
	 * </b>
	 * 
	 * @param message
	 *            the message to be enqueued
	 * @param producer
	 *            hold the queue name and addressing info of the producer of
	 *            this message.
	 * @param deliveryMode
	 *            see MantaAgentConstants for types
	 * @param priority
	 *            see MantaAgentConstants for types
	 * @param ackType
	 *            see MantaAgentConstants for types
	 * @param timeToLive
	 *            after this time this message should not be sent (millisec)
	 * @throws MantaException
	 *             if no such active topic
	 * @throws MantaException
	 *             if fail to send to server
	 * @see org.mr.kernel.services.ServiceProducer
	 * @see advertiseService(ServiceActor serviceActor)
	 * @see org.mr.core.protocol.MantaBusMessage
	 */
	public void enqueueMessage(MantaBusMessage message, ServiceProducer producer, byte deliveryMode, byte priority, long timeToLive) throws MantaException
	{
		String queueName = producer.getServiceName();
		long now = SystemTime.currentTimeMillis();
		VirtualQueuesManager queuesManager = singletonRepository.getVirtualQueuesManager();
		MantaService service = queuesManager.getQueueService(queueName);
		if (service == null)
		{
			String msg = "Queue was not found. Queue=" + queueName + ".";
			if (log.isDebugEnabled())
			{
				log.debug(msg);
			}
			throw new MantaException(msg, MantaException.ID_INVALID_ARGUMENTS);
		}
		QueueMaster master = queuesManager.getQueueMaster(queueName);
		if (master == null)
			try
			{
				if (log.isDebugEnabled())
				{
					log.debug("Waiting for queue coordinator to be created. Queue=" + queueName + ".");
				}// if
				long timeToWait = VirtualQueuesManager.getEnqueueWaitforCoordinator();
				if (timeToWait == -1)
				{
					// if timeout is -1 then take the ttl of the message
					timeToWait = timeToLive - now;
				}
				queuesManager.waitForQueueMaster(queueName, timeToWait);
				master = queuesManager.getQueueMaster(queueName);
			}
			catch (InterruptedException e1)
			{
				throw new MantaException("An error occured while waiting for queue coordinator to be created. Queue=" + queueName + ". " + e1.getMessage(), MantaException.ID_RECEIVE_GENERAL_FAIL);

			}
		if (master == null)
		{
			String msg = "Queue coordinator was not found. Queue=" + queueName + ".";
			if (log.isDebugEnabled())
			{
				log.debug(msg);
			}
			throw new MantaException(msg, MantaException.ID_INVALID_ARGUMENTS);
		}

		if (log.isDebugEnabled())
		{
			log.debug("Found queue coordinator for queue. Queue=" + queueName + ", Coordinator peer=" + master.getAgentName() + ", Coordinator ID=" + master.getId());
		}

		message.setPriority(priority);

		message.setDeliveryMode(deliveryMode);
		message.setValidUntil(timeToLive);

		message.addHeader(MantaBusMessageConsts.HEADER_NAME_LOGICAL_DESTINATION, queueName);

		if (message.getSource() == null)
		{
			MantaAddress add = producer;
			message.setSource(add);
		}

		// difine the control message TTL
		// now that we have the manta Message ready send it to the coordinator
		// the message to be sent to the other side
		MantaBusMessage controlMsg = MantaBusMessage.getInstance();

		controlMsg.setMessageType(MantaBusMessageConsts.MESSAGE_TYPE_CONTROL);

		// insert the control payload
		ControlSignal control = new ControlSignal(ControlSignal.OPERATION_TYPE_ENQUEUE);
		control.getParams().put(ControlSignal.ENQUEUED_MESSAGE, message);
		controlMsg.setPayload(control);
		// we return this blocker for the calling to wait on
		BlockingMessageListener blocker = new BlockingMessageListener(controlMsg);
		// blocker.setListenerString(queueName+msg.getMessageId() );
		blocker.setListenerString(queueName + control.getControlId());
		subscribeMessageListener(blocker, blocker.getListenerString());

		controlMsg.setRecipient(master);
		// difine the control message TTL
		long ctrlTTL = MantaAgentConstants.CONTROL_MESSAGES_TTL + now;
		if (timeToLive < ctrlTTL)
		{
			timeToLive = ctrlTTL;
		}

		controlMsg.addHeader(MantaBusMessageConsts.HEADER_NAME_LOGICAL_DESTINATION, queueName);
		if (log.isDebugEnabled())
		{
			log.debug("Sending control message to queue coordinator. Control Message ID=" + controlMsg.getMessageId() + ", Coordinator ID=" + master.getId());
		}

		send(controlMsg, producer, MantaAgentConstants.NON_PERSISTENT, MantaAgentConstants.HIGH, timeToLive);
		MantaBusMessage result;
		try
		{
			result = blocker.waitForResponse(timeToLive - now);
		}
		catch (InterruptedException e)
		{
			if (log.isDebugEnabled())
			{
				log.debug("An error occured while waiting for coordinator response to control message. Control Message ID=" + controlMsg.getMessageId() + ", Coordinator ID=" + master.getId(), e);
			}
			throw new MantaException("Error while sending message to queue. Queue=" + queueName, MantaException.ID_RECEIVE_GENERAL_FAIL);
		}
		unsubscribeMessageListener(blocker, blocker.getListenerString());
		if (queuesManager.isTempQueue(queueName) && !queuesManager.amIQueueMaster(queueName))
		{
			singletonRepository.getPostOffice().closeBox(master);
			queuesManager.closeQueue(queueName);
		}

		if (result == null)
		{
			if (log.isDebugEnabled())
			{
				log.debug("Queue coordinator did not respond to control message. Control Message ID=" + controlMsg.getMessageId() + ", Coordinator ID=" + master.getId());
			}
			throw new MantaException("Error while sending message to queue, Queue=" + queueName, MantaException.ID_RECEIVE_GENERAL_FAIL);
		}
		// Aviad added here throtoling mechanism
		byte enqRes = Byte.parseByte(result.getHeader(MantaBusMessageConsts.ENQUEUE_STATUS));
		if (enqRes == VirtualQueuesManager.ENQUEUE_FAIL)
		{
			int queueStrategy = Integer.parseInt(getSingletonRepository().getConfigManager().getStringProperty("jms.queue_overflow_strategy"));
			String overFlawMsg = "Queue overflow. Queue name=" + queueName + ", message ID " + message.getMessageId();
			if (queueStrategy == AbstractQueueService.THROW_EXCEPTION_STRATERGY)
			{
				throw new IllegalStateException(overFlawMsg);
			}
			else if (queueStrategy == AbstractQueueService.RETURN_WITHOUT_ENQUEUE_STRATERGY)
			{
				if (log.isWarnEnabled())
				{
					log.warn(overFlawMsg + ". Message droped.");
				}
			}
		}
		if (log.isDebugEnabled())
		{
			log.debug("Queue coordinator responded to control message. Control Message ID=" + controlMsg.getMessageId() + ", Coordinator ID=" + master.getId());
		}
	}// enqueueMessage

	/**
	 * Receives the next message on a queue, blocks until a message is dequeued
	 * from the queue and return to the invoker, the queue name is held in the
	 * ServiceConsumer object. <b>Note: 2 thread should not invoke this method
	 * with the same ServiceConsumer object </b> <b>Note: you MUST advertise
	 * yourself as queue consumer before invoking this method. </b>
	 * 
	 * @param consumer
	 *            holds the queue Name along with information to address the
	 *            queue message back to the invoker.
	 * @return a manta bus message from the remote queue
	 * @throws MantaException
	 *             if fail to receive for any reason
	 * @see org.mr.kernel.services.ServiceConsumer
	 * @see advertiseService(ServiceActor serviceActor)
	 * @see org.mr.core.protocol.MantaBusMessage
	 */
	public MantaBusMessage receive(ServiceConsumer consumer) throws MantaException
	{
		return receive(consumer, Long.MAX_VALUE);
	}

	/**
	 * Receives the next message on a queue, blocks until a message is dequeued
	 * from the queue and return to the invoker or until timeout has expired ,
	 * the queue name is held in the ServiceConsumer object. <b>Note: 2 thread
	 * should not invoke this method with the same ServiceConsumer object </b>
	 * <b>Note: you MUST advertise yourself as queue consumer before invoking
	 * this method. </b>
	 * 
	 * @param consumer
	 *            holds the queue Name along with information to address the
	 *            queue message back to the invoker.
	 * @param timeout
	 *            until when to wait for response
	 * @return a manta bus message from the remote queue
	 * @throws MantaException
	 *             if fail to receive for any reason
	 * @see org.mr.kernel.services.ServiceConsumer
	 * @see advertiseService(ServiceActor serviceActor)
	 * @see org.mr.core.protocol.MantaBusMessage
	 */
	public MantaBusMessage receive(ServiceConsumer consumer, long timeout) throws MantaException
	{
		String queueName = consumer.getServiceName();
		WorldModeler world = singletonRepository.getWorldModeler();

		if (timeout < 0)
		{
			timeout = Long.MAX_VALUE;
		}

		MantaBusMessage result = null;
		VirtualQueuesManager queuesManager = singletonRepository.getVirtualQueuesManager();

		// add yourself to the queue receiver list
		BlockingMessageListener blocker = registerToQueue(consumer, 1);
		// if blocker is null there are no known master for this queue
		// we need to wait for a master to advertise itself
		if (blocker == null)
		{
			try
			{
				queuesManager.waitForQueueMaster(queueName, timeout);
			}
			catch (InterruptedException e1)
			{
				throw new MantaException("This should not happen InterruptedException on service waitForProducerChange" + e1.toString(), MantaException.ID_RECEIVE_GENERAL_FAIL);
			}
			blocker = registerToQueue(consumer, 1);
		}
		// if there is still no master for the queue return null
		if (blocker == null)
			return null;
		// here we wait for response from the master
		try
		{
			result = blocker.waitForResponse(timeout);
		}
		catch (InterruptedException e)
		{
			if (log.isErrorEnabled())
			{
				log.error("Got exception while waiting on receive. ", e);
			}
		}

		unsubscribeMessageListener(blocker, blocker.getListenerString());
		queuesManager.getSubscriberManager(queueName).removeSubscribeToQueue(consumer);
		// remove yourself from the queue receiver list
		// this is done if we got timed out before we got a queue element
		if (result == null)
		{
			unregisterFromQueue(consumer, blocker);
		}// if

		return result;

	}

	/**
	 * Returns an Enumeration of the underline remote queue <b>Note: 2 thread
	 * should not invoke this method with the same ServiceConsumer object </b>
	 * <b>Note: you MUST advertise yourself as queue consumer before invoking
	 * this method. </b>
	 * 
	 * @param consumer
	 *            holds the queue Name along with information to address the
	 *            queue message back to the invoker.
	 * @return an Enumeration of the underline remote queue
	 * @throws MantaException
	 *             if service is null or not queue
	 * @see org.mr.kernel.services.ServiceConsumer
	 * @see advertiseService(ServiceActor serviceActor)
	 */
	public Enumeration peekAtQueue(ServiceConsumer consumer) throws MantaException
	{
		String queueName = consumer.getServiceName();
		VirtualQueuesManager queuesManager = singletonRepository.getVirtualQueuesManager();
		MantaService service = queuesManager.getQueueService(queueName);
		if (service == null || consumer.getServiceType() != MantaService.SERVICE_TYPE_QUEUE)
		{
			throw new MantaException("No such Service " + queueName, MantaException.ID_INVALID_ARGUMENTS);
		}
		// check if the queue coordinator is accessible
		QueueMaster master = queuesManager.getQueueMaster(queueName);
		if (master != null && !singletonRepository.getNetworkManager().isAccessible(master.getAgentName()))
		{
			log.error("Queue coordinator not accessible, this might me configuration problem , coordinator=" + master.getAgentName());
			throw new MantaException("Queue coordinator not accessible  " + queueName, MantaException.ID_INVALID_ARGUMENTS);
		}
		return new RemoteQueueEnumeration(consumer);
	}

	/**
	 * Receives the next message if one is immediately available. Else the
	 * client returns immediately. the queue name is held in the ServiceConsumer
	 * object. <b>Note: 2 thread should not invoke this method with the same
	 * ServiceConsumer object </b> <b>Note: you MUST advertise yourself as queue
	 * consumer before invoking this method. </b>
	 * 
	 * @param consumer
	 *            holds the queue Name along with information to address the
	 *            queue message back to the invoker.
	 * @return MantaBusMessage the message that was dequeued from the queue or
	 *         null if non dequeued
	 * @throws MantaException
	 *             if fail to receive for any reason
	 * @see org.mr.kernel.services.ServiceConsumer
	 * @see advertiseService(ServiceActor serviceActor)
	 * @see org.mr.core.protocol.MantaBusMessage
	 */
	public MantaBusMessage receiveNoWait(ServiceConsumer consumer) throws MantaException
	{
		String queueName = consumer.getServiceName();
		VirtualQueuesManager queuesManager = singletonRepository.getVirtualQueuesManager();
		MantaService service = queuesManager.getQueueService(queueName);
		if (service == null)
		{
			throw new MantaException("No such Service " + queueName, MantaException.ID_INVALID_ARGUMENTS);
		}
		QueueMaster master = queuesManager.getQueueMaster(queueName);
		// if there is no coordinator then return empty handed
		if (master == null || master.getValidUntil() < SystemTime.currentTimeMillis())
		{
			return null;
		}
		// we have producers go on
		MantaBusMessage msg = null;
		BlockingMessageListener blocker = registerToQueue(consumer, 0);
		// if there are no coordinator for this queue return null
		if (blocker == null)
			return null;

		try
		{
			// the response should come with null, we should not wait very long
			msg = blocker.waitForResponse(10000);
			if (msg != null)
			{
				if (msg.getHeader(MantaBusMessageConsts.HEADER_NAME_IS_EMPTY) != null)
				{
					// we should return null if header empty response is null
					msg = null;
				}
			}
		}
		catch (InterruptedException e)
		{
			if (log.isErrorEnabled())
			{
				log.error("Got an exception while waiting on receiveNoWait. ", e);
			}

		}
		unsubscribeMessageListener(blocker, blocker.getListenerString());
		queuesManager.getSubscriberManager(consumer.getServiceName()).removeSubscribeToQueue(consumer);

		return msg;
	}// receiveNoWait

	/**
	 * Registers a agent to listener on a remote queue
	 * 
	 * @param consumer
	 *            the consumer of the queue (holds the queue name and more info)
	 * @param numberOfRecive
	 *            -after this number of message this agent should not be
	 *            notified on and more enqueue messages if numberOfRecive == 0
	 *            then this is a no wait receive
	 */
	private BlockingMessageListener registerToQueue(ServiceConsumer consumer, long numberOfReceive) throws MantaException
	{
		String queueName = consumer.getServiceName();
		VirtualQueuesManager queuesManager = singletonRepository.getVirtualQueuesManager();
		MantaService service = queuesManager.getQueueService(queueName);
		if (service == null || service.getServiceType() != MantaService.SERVICE_TYPE_QUEUE)
		{
			throw new MantaException("No such queue Service " + queueName, MantaException.ID_INVALID_ARGUMENTS);
		}

		BlockingMessageListener blocker = new BlockingMessageListener();
		queuesManager.getSubscriberManager(consumer.getServiceName()).subscribeToQueue(consumer, blocker, numberOfReceive);

		return blocker;
	}// registerToQueue

	/**
	 * Removes this agent from listening to a given remote queue
	 * 
	 * @param listener
	 *            the local listener
	 * @param consumer
	 *            the consumer the registered to the queue
	 */
	private void unregisterFromQueue(ServiceConsumer consumer, IMessageListener listener) throws MantaException
	{
		String queueName = consumer.getServiceName();
		VirtualQueuesManager queuesManager = singletonRepository.getVirtualQueuesManager();
		MantaService service = queuesManager.getQueueService(queueName);
		if (service == null || service.getServiceType() != MantaService.SERVICE_TYPE_QUEUE)
		{
			throw new MantaException("No such queue Service " + queueName, MantaException.ID_INVALID_ARGUMENTS);
		}

		queuesManager.getSubscriberManager(consumer.getServiceName()).unregisterFromQueue(consumer, listener);

	}

	/**
	 * Returns a copy of the content of a remote queue. <b>Note: 2 thread should
	 * not invoke this method with the same ServiceConsumer object </b> <b>Note:
	 * you MUST advertise yourself as queue consumer before invoking this
	 * method. </b>
	 * 
	 * @param consumer
	 *            holds the queue Name along with information to address the
	 *            queue message back to the invoker.
	 * @return a copy of a remote queue messages: LinkedList with
	 *         MantaBusMessages
	 * @throws MantaException
	 *             if service == null or remote error happens
	 * @see org.mr.kernel.services.ServiceConsumer
	 * @see advertiseService(ServiceActor serviceActor)
	 * @see org.mr.core.protocol.MantaBusMessage
	 */
	public ArrayList CopyQueueContent(ServiceConsumer consumer) throws MantaException
	{
		String queueName = consumer.getServiceName();
		VirtualQueuesManager queuesManager = singletonRepository.getVirtualQueuesManager();
		MantaService service = queuesManager.getQueueService(queueName);
		if (service == null)
		{
			throw new MantaException("No such Service " + queueName, MantaException.ID_INVALID_ARGUMENTS);
		}
		ArrayList result = null;
		// find the destination
		QueueMaster master = queuesManager.getQueueMaster(queueName);
		// if master is no where to be found return null
		if (master == null)
			return new ArrayList();

		// go on to copy from producers
		MantaBusMessage msg = MantaBusMessage.getInstance();
		BlockingMessageListener listener = new BlockingMessageListener(msg);
		listener.setListenerString(queueName + consumer.getId());
		subscribeMessageListener(listener, listener.getListenerString());
		msg.setMessageType(MantaBusMessageConsts.MESSAGE_TYPE_CONTROL);
		// insert the control payload
		ControlSignal control = new ControlSignal(ControlSignal.OPERATION_TYPE_GET_QUEUE_COPY);
		msg.setPayload(control);

		msg.setRecipient(master);

		msg.addHeader(MantaBusMessageConsts.HEADER_NAME_LOGICAL_DESTINATION, queueName);

		send(msg, consumer, MantaAgentConstants.NON_PERSISTENT, MantaAgentConstants.HIGH, MantaAgentConstants.CONTROL_MESSAGES_TTL + SystemTime.gmtCurrentTimeMillis());

		try
		{
			result = (ArrayList) listener.waitForResponse(Long.MAX_VALUE).getPayload();
		}
		catch (Exception e)
		{
			if (log.isErrorEnabled())
			{
				log.error("Error in copying remote queue, return with empty ArrayList.", e);
			}

		}
		if (result == null)
		{
			result = new ArrayList();
		}

		unsubscribeMessageListener(listener, listener.getListenerString());
		return result;

	}// CopyQueueContent

	/**
	 * Tells the queue coordinator that this service consumer wishes to do
	 * multipul Receives (stoped when calling the
	 * unsubscribeMessageListenerToQueue) when a message is ready on the queue
	 * side and the coordinator sends the message to the consumer the onMessage
	 * method will be invoked of the given listener in simple terms this
	 * transfers the pull of the queue to push <b>Note: 2 thread should not
	 * invoke this method with the same ServiceConsumer object </b> <b>Note: you
	 * MUST advertise yourself as queue consumer before invoking this method.
	 * </b>
	 * 
	 * @param consumer
	 *            holds the queue Name along with information to address the
	 *            queue message back to the invoker.
	 * @param listener
	 *            from the interface IMessageListener
	 * @throws MantaException
	 *             in Communication exception
	 */
	public void subscribeToQueue(ServiceConsumer consumer, IMessageListener listener) throws MantaException
	{
		String queueName = consumer.getServiceName();
		VirtualQueuesManager queuesManager = singletonRepository.getVirtualQueuesManager();
		MantaService service = queuesManager.getQueueService(queueName);
		if (service == null || service.getServiceType() != MantaService.SERVICE_TYPE_QUEUE)
		{
			throw new MantaException("No such queue Service " + queueName, MantaException.ID_INVALID_ARGUMENTS);
		}

		// find the destinations
		queuesManager.getSubscriberManager(consumer.getServiceName()).subscribeToQueue(consumer, listener, Long.MAX_VALUE);

	}// subscribeMessageListenerToQueue

	/**
	 * Tells the queue coordinator that the request to no multiple receives is
	 * no longer needed and the receiver should be removed after this point the
	 * listener will not be invoked with the onMessage method in simple terms
	 * this undoes what subscribeMessageListenerToQueue has done
	 * 
	 * @param consumer
	 *            the consumer that wishes to stop getting messages from the
	 *            queue
	 * @param listener
	 *            the listener that is going to be remove from the listener to
	 *            this queue
	 * @throws MantaException
	 *             if fails to unregister
	 */

	public void unsubscribeFromQueue(ServiceConsumer consumer, IMessageListener listener) throws MantaException
	{
		this.unregisterFromQueue(consumer, listener);
	}

	// /////////////////////////////////////////////////////////
	// Topics
	// /////////////////////////////////////////////////////////
	/**
	 * Publishes the message to a topic (deliveryMode , priority , timeToLive,
	 * and other properties are taken from default) <b>Note: you MUST advertise
	 * yourself as topic producer before invoking this method. </b>
	 * 
	 * @param message
	 *            the message to be published
	 * @param producer
	 *            hold the topic name and addressing info of the producer of
	 *            this message.
	 * @see org.mr.kernel.services.ServiceProducer
	 * @see advertiseService(ServiceActor serviceActor)
	 * @see org.mr.core.protocol.MantaBusMessage
	 * 
	 */
	public void publish(MantaBusMessage message, ServiceProducer producer) throws MantaException
	{
		publish(message, producer, message.getDeliveryMode(), message.getPriority(), message.getValidUntil());
	}

	/**
	 * Publishes a message to the topic, specifying delivery mode, priority, and
	 * time to live. <b>Note: you MUST advertise yourself as topic producer
	 * before invoking this method. </b>
	 * 
	 * @param message
	 *            the message to be published
	 * @param producer
	 *            hold the topic name and addressing info of the producer of
	 *            this message.
	 * @param deliveryMode
	 *            see MantaAgentConstants for types
	 * @param priority
	 *            see MantaAgentConstants for types
	 * @param expiration
	 *            after this time this message should not be sent (millisec GMT)
	 * @see org.mr.kernel.services.ServiceProducer
	 * @see advertiseService(ServiceActor serviceActor)
	 * @see org.mr.core.protocol.MantaBusMessage
	 */
	public void publish(MantaBusMessage message, ServiceProducer producer, byte deliveryMode, byte priority, long expiration) throws MantaException
	{
		String topic = producer.getServiceName();
		VirtualTopicManager topicManager = singletonRepository.getVirtualTopicManager();

		if (!topicManager.hasTopic(topic))
		{
			throw new MantaException("No such active topic Service " + topic, MantaException.ID_INVALID_ARGUMENTS);
		}
		try
		{
			topicManager.publish(topic, message, producer, deliveryMode, priority, expiration);
		}
		catch (IOException e)
		{
			throw new MantaException("failed to publish " + e.getLocalizedMessage(), MantaException.ID_INVALID_ARGUMENTS);
		}

	}// Publish

	/**
	 * Sets a Message Listener, message listeners are called back by the manta's
	 * layer logic upon receiving a message. if a mantaBusMessage arrives with a
	 * destination that is this given destination the IMessageListener onMessage
	 * method will be called. this method should be used on topics only
	 * 
	 * @param listener
	 *            from the interface IMessageListener
	 * @see IMessageListener
	 */
	public void subscribeToTopic(IMessageListener listener, String topic) throws MantaException
	{
		VirtualTopicManager topicManager = singletonRepository.getVirtualTopicManager();
		if (!topicManager.hasTopic(topic))
		{
			throw new MantaException("No such active topic Service " + topic, MantaException.ID_INVALID_ARGUMENTS);
		}
		subscribeMessageListener(listener, topic);
	}

	/**
	 * Removes a Message Listener from the agent logic. This can be thought of
	 * as a unsubscribing to some Template (topic)
	 * 
	 * @param listener
	 *            from the interface IMessageListener
	 * @see IMessageListener
	 */
	public void unsubscribeFromTopic(IMessageListener listener, String topic)
	{
		unsubscribeMessageListener(listener, topic);
	}

	// /////////////////////
	// / general
	// ////////////////////
	/**
	 * for internal use only. Sets a Message Listener, message listeners are
	 * called back by the manta's layer logic upon receiving a message. if a
	 * mantaBusMessage arrives with a destination that is this given destination
	 * the IMessageListener onMessage method will be called
	 * 
	 * @param listener
	 *            from the interface IMessageListener
	 * @see org.mr.IMessageListener
	 */
	public void subscribeMessageListener(IMessageListener listener, String destination)
	{
		singletonRepository.getIncomingClientMessageRouter().addIncommingClientMessageListener(destination, listener);
	}

	/**
	 * for internal use only Removes a Message Listener from the agent logic.
	 * This can be thought of as a unsubscribing to some Template (topic)
	 * 
	 * @param listener
	 *            from the interface IMessageListener
	 * @see IMessageListener
	 */
	public void unsubscribeMessageListener(IMessageListener listener, String destination)
	{
		singletonRepository.getIncomingClientMessageRouter().removeIncomingClientMessageListener(destination, listener);
	}

	/**
	 * Sends a message to a remote agent We assume that the message has a valid
	 * headers like destination source and so on for the message to be received
	 * on the other side a IMessageLister should be up and listening to the
	 * logical destination of this message. <p/> MantaBusMessage message must
	 * hold all the proper info (ack type , priority ....) for the message to be
	 * sent
	 * 
	 * @param message
	 *            the message to be sent
	 * @param sender
	 *            the sender of the message holds the return info of the message
	 * @see org.mr.core.protocol.MantaBusMessage
	 * @see org.mr.core.protocol.MantaBusMessageConsts
	 * @see org.mr.kernel.services.ServiceProducer
	 * @see org.mr.kernel.services.ServiceConsumer
	 * @see setMessageListener(IMessageListener listener ,String destination)
	 */
	public void send(MantaBusMessage message, MantaAddress sender)
	{
		if (message.getSource() == null)
		{
			message.setSource(sender);
		}

		MessageManipulator mm = singletonRepository.getMessageManipulator();
		if (mm != null)
		{
			message = mm.manipulate(message, null, MessageManipulator.OUTGOING);
		}
		// send the message
		getSingletonRepository().getPostOffice().SendMessage(message);
	}

	/**
	 * sends a message to an agent , specifying delivery mode, priority and time
	 * to live.
	 * 
	 * @param message
	 *            the message to be sent *
	 * @param sender
	 *            the sender of the message holds the return info of the message
	 * @param deliveryMode
	 *            see MantaAgentConstants for valid values
	 * @param priority
	 *            see MantaAgentConstants for valid values
	 * @param expiration
	 *            in millisec GMT
	 * @see org.mr.core.protocol.MantaBusMessage
	 * @see org.mr.core.protocol.MantaBusMessageConsts
	 * @see org.mr.kernel.services.ServiceProducer
	 * @see org.mr.kernel.services.ServiceConsumer
	 * @see setMessageListener(IMessageListener listener ,String destination)
	 */
	public void send(MantaBusMessage message, MantaAddress sender, byte deliveryMode, byte priority, long expiration)
	{

		message.setPriority(priority);

		message.setDeliveryMode(deliveryMode);

		if (message.getValidUntil() < 1)
		{
			// if ValidUntil was not set set it
			message.setValidUntil(expiration);

		}

		send(message, sender);

	}

	// ////////////////////////////////////
	// acks
	// ///////////////////////////////////

	/**
	 * Tells the sender of a message that this receiver has got the message and
	 * not resending to this receive address is needed. <p/> the point in the
	 * code where this method is called after the receiving of a message depends
	 * if the ack type of the message
	 * 
	 * @param messageToAckToo
	 *            the original message that we want to ack to
	 * @see org.mr.core.protocol.MantaBusMessage
	 * @see org.mr.core.protocol.MantaBusMessageConsts
	 */
	public void ack(MantaBusMessage messageToAckToo)
	{
		if (messageToAckToo != null && messageToAckToo.getRecipient() != null && messageToAckToo.getSource() != null)
		{
			MantaBusMessage reply = MantaBusMessageUtil.createACKMessage(messageToAckToo);

			// set the ack to header
			String messageId = messageToAckToo.getMessageId();
			reply.addHeader(MantaBusMessageConsts.HEADER_NAME_ACK_RESPONSE_REFERENCE, messageId);
			reply.removeHeader(MantaBusMessageConsts.HEADER_NAME_LOGICAL_DESTINATION);
			// Aviad use startupLogger
			// if (log.isDebugEnabled()) {
			// log.debug("Sending ACK: Message ID="+
			// reply.getMessageId()+
			// ", responding to message "+
			// messageId);
			// }
			StartupLogger.log.debug("Sending ACK: Message ID=" + reply.getMessageId() + ", responding to message " + messageId, "MantaAgent");
			send(reply, messageToAckToo.getRecipient(), MantaAgentConstants.NON_PERSISTENT, MantaAgentConstants.HIGH, MantaAgentConstants.ACK_TTL + SystemTime.gmtCurrentTimeMillis());
		}

	}

	/**
	 * Tells the sender of a message that no receiver has got the message and
	 * the message should be returned to the queue.
	 * 
	 * @param messageToAckTo
	 *            the original message that we want to ack to
	 * @see org.mr.core.protocol.MantaBusMessage
	 * @see org.mr.core.protocol.MantaBusMessageConsts
	 */
	public void ackReject(MantaBusMessage messageToAckTo)
	{
		if (messageToAckTo != null && messageToAckTo.getRecipient() != null && messageToAckTo.getSource() != null)
		{
			MantaBusMessage reply = MantaBusMessageUtil.createACKMessage(messageToAckTo);

			// set the ack to header
			String messageId = messageToAckTo.getMessageId();
			reply.addHeader(MantaBusMessageConsts.HEADER_NAME_ACK_REJECT_RESPONSE_REFERENCE, messageId);
			reply.removeHeader(MantaBusMessageConsts.HEADER_NAME_LOGICAL_DESTINATION);
			if (log.isDebugEnabled())
			{
				log.debug("Sending Ack Reject: Message ID=" + reply.getMessageId() + ", responding to message " + messageId);
			}
			send(reply, messageToAckTo.getRecipient(), MantaAgentConstants.NON_PERSISTENT, MantaAgentConstants.HIGH, MantaAgentConstants.ACK_TTL + SystemTime.gmtCurrentTimeMillis());
		}
	}

	/**
	 * do not use this method it is for inner agent use
	 */
	public void gotAck(String ackedMessageId, MantaAddress source)
	{
		MantaBusMessage msg = singletonRepository.getPostOffice().gotAck(ackedMessageId, source);
		if (msg == null)
			return;

		singletonRepository.getDeliveryAckNotifier().gotAck(msg, source);

	}// gotAck

	/**
	 * do not use this method it is for inner agent use
	 */
	public void gotAckReject(String ackedMessageId, MantaAddress source)
	{
		// it don't matter to the post office! ACK, Reject, whatever:
		// it only wants to delete the saveed message.
		MantaBusMessage msg = singletonRepository.getPostOffice().gotAckReject(ackedMessageId, source);
		if (msg != null)
		{
			singletonRepository.getDeliveryAckNotifier().gotAckReject(msg, source);
		}
	}// gotAck

	/**
	 * @param ackListener
	 *            The ackListener to set.
	 */
	public void setAckListener(DeliveryAckListener ackListener)
	{
		if (singletonRepository.getDeliveryAckNotifier() == null)
		{
			singletonRepository.setDeliveryAckNotifier(new DeliveryAckNotifier());
		}
		singletonRepository.getDeliveryAckNotifier().setGlobalListener(ackListener);
	}

	// /////////////////////////////////////////////////////////
	// General methods relating to service operations
	// /////////////////////////////////////////////////////////

	/**
	 * Tells all the other layer that this layer is a producer or a consumer
	 * (role) of a queue or a topic (service) this method MUST be called before
	 * you perform operations on a service. example: before publishing stock
	 * messages to a stock topic you call this method with a producer role, then
	 * you can publish as many message as you want on this topic. if you publish
	 * a message to a topic without using this method no one will consume this
	 * message and it will be lost. <p/> For a given service and a given role a
	 * one-time advertise should be done unless you recall the role using the
	 * recallService(ServiceActor serviceActor). You could at any time recall
	 * your role to a service.
	 * 
	 * @param serviceActor
	 *            hold the service name and the role of the invoker of this
	 *            method
	 * @throws MantaException
	 *             if service not found
	 * @see org.mr.kernel.services.ServiceProducer
	 * @see org.mr.kernel.services.ServiceConsumer
	 */
	public void advertiseService(ServiceActor serviceActor) throws MantaException
	{

		WorldModeler world = singletonRepository.getWorldModeler();

		// activate the service
		if (serviceActor.getServiceType() == MantaService.SERVICE_TYPE_QUEUE)
		{
			// this is a queue
			VirtualQueuesManager vqm = singletonRepository.getVirtualQueuesManager();
			MantaService queue = vqm.getQueueService(serviceActor.getServiceName());
			if (queue == null)
			{
				throw new MantaException("No such Queue " + serviceActor.getServiceName(), MantaException.ID_INVALID_ARGUMENTS);
			}

			if (serviceActor.getType() == ServiceActor.COORDINATOR)
			{
				world.addCoordinatedService(queue);
				QueueMaster coordinator = (QueueMaster) serviceActor;
				coordinator.setValidUntil(Long.MAX_VALUE);
				vqm.setQueueMaster(serviceActor.getServiceName(), (QueueMaster) serviceActor);
				// This is a patch:
				// In case of queue coordinator we want to advertise before
				// recovering
				// the messages. That prevents other peers to advertise them
				// selves
				// as coordinators while this agent recovers.
				if (!serviceActor.getServiceName().startsWith(MantaConnection.TMP_DESTINATION_PREFIX))
				{
					singletonRepository.getServiceActorControlCenter().advertiseService(serviceActor, this);
				}
				// activate the queue
				vqm.active(serviceActor.getServiceName());
				return;
			}
			else if (serviceActor.getType() == ServiceActor.PRODUCER)
			{
				world.addProducedService(queue);
				queue.addProducer((ServiceProducer) serviceActor);
			}
			else if (serviceActor.getType() == ServiceActor.CONSUMER)
			{
				world.addConsumedServices(queue);
				queue.addConsumer((ServiceConsumer) serviceActor);
			}
			if (vqm.isTempQueue(queue.getServiceName()))
			{
				// do not advertise temp roles
				return;
			}

		}
		else
		{
			// this is a topic
			VirtualTopicManager vtm = getSingletonRepository().getVirtualTopicManager();

			if (serviceActor.getType() == ServiceActor.PRODUCER)
			{
				MantaService topic = (MantaService) getService(serviceActor.getServiceName(), MantaService.SERVICE_TYPE_TOPIC);
				if (topic == null)
				{
					throw new MantaException("No such Topic " + serviceActor.getServiceName(), MantaException.ID_INVALID_ARGUMENTS);
				}

				singletonRepository.getWorldModeler().addProducedService(topic);
				topic.addProducer((ServiceProducer) serviceActor);
			}
			else if (serviceActor.getType() == ServiceActor.CONSUMER)
			{
				singletonRepository.getVirtualTopicManager().addConsumer((ServiceConsumer) serviceActor);
			}
		}
		if (serviceActor.getServiceType() == MantaService.SERVICE_TYPE_TOPIC || !serviceActor.getServiceName().startsWith(MantaConnection.TMP_DESTINATION_PREFIX))
		{
			// temp queue roles should not be advertise
			singletonRepository.getServiceActorControlCenter().advertiseService(serviceActor, this);
		}
	}// advertiseService

	/**
	 * Recalls a previously advertised role (consumer or producer) in service
	 * (queue or a topic) which is not longer accessible from this layer. if at
	 * runtime you want to stop been a consumer or a producer (i.e when going
	 * offline) you SHOULD use this method to recalls the role in the service
	 * 
	 * @throws MantaException
	 *             if service not found
	 * @see org.mr.kernel.services.ServiceProducer
	 * @see org.mr.kernel.services.ServiceConsumer
	 */
	public void recallService(ServiceActor serviceActor) throws MantaException
	{

		WorldModeler world = singletonRepository.getWorldModeler();
		String serviceName = serviceActor.getServiceName();
		if (serviceActor.getType() == ServiceActor.PRODUCER)
		{

			MantaService service = getService(serviceName, serviceActor.getServiceType());
			if (service == null)
			{
				throw new MantaException("No such Service " + serviceName, MantaException.ID_INVALID_ARGUMENTS);
			}

			service.removeProducer((ServiceProducer) serviceActor);

			if (service.getProducersByAgentId(getAgentName()).isEmpty())
			{
				world.removeProducedService(service);
			}
		}
		else if (serviceActor.getType() == ServiceActor.CONSUMER)
		{
			if (serviceActor.getServiceType() == MantaService.SERVICE_TYPE_TOPIC)
			{
				singletonRepository.getVirtualTopicManager().removeConsumer((ServiceConsumer) serviceActor);
			}
			else
			{
				MantaService service = getService(serviceName, serviceActor.getServiceType());
				if (service == null)
				{
					throw new MantaException("No such Service " + serviceName, MantaException.ID_INVALID_ARGUMENTS);
				}

				service.removeConsumer((ServiceConsumer) serviceActor);

				// this is a queue lets remove its subscription to the queue
				ServiceConsumer consumer = (ServiceConsumer) serviceActor;
				singletonRepository.getVirtualQueuesManager().getSubscriberManager(serviceName).removeSubscribeToQueue(consumer);

				if (service.getConsumersByAgentId(getAgentName()).isEmpty())
				{
					world.removeConsumedServices(service);
				}
			}

		}
		else if (serviceActor.getType() == ServiceActor.COORDINATOR)
		{
			MantaService service = getService(serviceName, serviceActor.getServiceType());
			if (service == null)
			{
				throw new MantaException("No such Service " + serviceName, MantaException.ID_INVALID_ARGUMENTS);
			}

			if (singletonRepository.getVirtualQueuesManager().amIQueueMaster(serviceName) == false)
			{
				// i am not a coordinator of this queue
				return;
			}
			getSingletonRepository().getPostOffice().handleCoordinatorDown((QueueMaster) serviceActor, null);
			world.removeCoordinatedService(service);
		}
		if (!serviceActor.getServiceName().startsWith(MantaConnection.TMP_DESTINATION_PREFIX))
		{
			// no need to recall temp roles
			singletonRepository.getServiceActorControlCenter().recallService(serviceActor, this);
		}
	}// recallService

	/**
	 * Recalls a previously advertised durable subscriber role (consumer ) in
	 * topic service which is not longer accessible from this layer. if at
	 * runtime you want to stop been a durable subscriber (consumer) (i.e when
	 * going offline and do not want to get offline messages) you SHOULD use
	 * this method to recalls the role in the service
	 * 
	 * @throws MantaException
	 *             if service not found
	 * @see org.mr.kernel.services.ServiceConsumer
	 */
	public void recallDurableSubscription(ServiceActor durableSubscription) throws MantaException
	{

		WorldModeler world = singletonRepository.getWorldModeler();
		MantaService service = (MantaService) getService(durableSubscription.getServiceName(), durableSubscription.getServiceType());
		if (service == null)
		{
			throw new MantaException("No such Service " + durableSubscription.getServiceName(), MantaException.ID_INVALID_ARGUMENTS);
		}

		singletonRepository.getVirtualTopicManager().removeDurableConsumer(service.getServiceName(), (ServiceConsumer) durableSubscription);

		if (service.getConsumersByAgentId(getAgentName()).isEmpty())
		{
			world.removeConsumedServices(service);
		}

		singletonRepository.getServiceActorControlCenter().recallDurableSubscription(durableSubscription, this);

	}// recallService

	/**
	 * @return a new manta bus message with a new id
	 * @see org.mr.core.protocol.MantaBusMessage
	 * @see org.mr.core.protocol.MantaBusMessageConsts
	 */
	public MantaBusMessage getMantaBusMessage()
	{
		return MantaBusMessage.getInstance();
	}

	/**
	 * returns the service object for this name trys to create it if not found
	 * or null if not found and can't be created. service object can be
	 * :QueueService or TopicService
	 * 
	 * @param name
	 *            the name of the Queue or Topic
	 * @param serviceType
	 *            needed for dynamic service creation can be
	 *            MantaService.SERVICE_TYPE_QUEUE or SERVICE_TYPE_TOPIC
	 * @return the service object for this name
	 * @see org.mr.kernel.services.queues.QueueService
	 * @see org.mr.kernel.services.topic.TopicService
	 */
	public MantaService getService(String name, byte serviceType)
	{
		MantaService result = null;
		if (serviceType == MantaService.SERVICE_TYPE_QUEUE)
		{
			result = singletonRepository.getVirtualQueuesManager().getQueueService(name);
		}
		else
		{
			result = singletonRepository.getVirtualTopicManager().getTopicService(name);
		}
		return result;
	}

	/**
	 * returns true if service is in the world modeler else false
	 * 
	 * @param service
	 *            the name of the Queue or Topic
	 * @return true if service is in the world modeler else false
	 * @see org.mr.kernel.services.queues.QueueService
	 * @see org.mr.kernel.services.topic.TopicService
	 */
	public boolean containsService(String service)
	{
		return singletonRepository.getWorldModeler().containsService(singletonRepository.getWorldModeler().getDefaultDomainName(), service);
	}

	/**
	 * DO NOT USE for internal use only
	 * 
	 * @return SingletonRepository hold all the singletons in the Singletons in
	 *         the system
	 */
	public SingletonRepository getSingletonRepository()
	{
		return singletonRepository;
	}// getSingletoneRepository

	/**
	 * @return a one time unique message id
	 */
	public String getMessageId()
	{
		return String.valueOf(UniqueIDGenerator.getNextMessageID());
	}

	/**
	 * gets the agent name of the current Transport layer
	 * 
	 * @return the agent name of the current Transport layer
	 */
	public String getAgentName()
	{

		return myName;
	}

	/**
	 * gets the domain name of the current Transport layer
	 * 
	 * @return the domain name of the current Transport layer
	 */
	public String getDomainName()
	{
		return singletonRepository.getWorldModeler().getDefaultDomainName();
	}

	protected DynamicRepository getDynamicRepository()
	{
		return dynamicRepository;
	}

	/**
	 * This method is used by MantaConnectionFactory to set the configuration
	 * DOm element, in case a configuration file is not used.
	 * 
	 * @param element
	 *            Configuration XML given as a DOM element
	 */
	public static void setConfiguration(Element element)
	{
		configurationElement = element;
	}

	public static boolean isStarted()
	{
		return started;
	}
}// MantaAgent
