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
 * The Initial Developer of the Original Code is Amir Shevat.
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
/*
 * Created on Feb 2, 2004
 * Manta LTD
 */
package org.mr.kernel.services.topics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.mr.MantaAgent;
import org.mr.MantaAgentConstants;
import org.mr.MantaException;
import org.mr.kernel.delivery.PostOffice;
import org.mr.kernel.delivery.PostOfficeBox;
import org.mr.kernel.services.MantaService;
import org.mr.kernel.services.PayLoadSelector;
import org.mr.kernel.services.SelectorsManager;
import org.mr.kernel.services.ServiceActorControlCenter;
import org.mr.kernel.services.ServiceConsumer;
import org.mr.kernel.services.ServiceProducer;
import org.mr.core.configuration.ConfigManager;
import org.mr.core.persistent.PersistentMap;
import org.mr.core.persistent.PersistentManager;
import org.mr.core.protocol.MantaBusMessage;
import org.mr.core.protocol.MantaBusMessageConsts;
import org.mr.core.protocol.RecipientAddress;
import org.mr.core.util.StringUtils;

/**
 * Created Feb 2, 2004
 * Ver 1.0
 * TopicService - is a meny to meny messageing service
 * where N consumers listen to a topic and N producers send messages on the topic
 * all messages sent by the producers are received by the consumers (see JMS spec)
 * @author Amir Shevat
 *
 *
 */
 class TopicService extends MantaService implements TopicServiceMBean{

    private static String HIERARCHY_DELIMITER = "~";
	PersistentMap subscribers;
	protected Log log;
	private boolean pause = false;

	static {
		ConfigManager config = MantaAgent.getInstance().getSingletonRepository().getConfigManager();
		HIERARCHY_DELIMITER = config.getStringProperty("persistency.hierarchy_delimiter", HIERARCHY_DELIMITER);
	}
	/**
	 *
	 * Constractor for TopicService
	 * @param serviceName the name of the topic
	 */
	public TopicService(String serviceName) {
		super(serviceName);
		log=LogFactory.getLog("Topic:"+serviceName);
		subscribers = new PersistentMap(PersistentManager.SUBSCRIBERS_PERSISTENT_PREFIX + cleanupServiceName(serviceName), false, true);
		ArrayList subscribersList = new ArrayList();
		synchronized(subscribers){
			subscribersList.addAll(subscribers.values());
		}
		int size = subscribersList.size();
		for (int i = 0; i < size; i++) {
			ServiceConsumer durable = (ServiceConsumer) subscribersList.get(i);
			consumers.add(durable);
			serviceActorMap.put(durable.getId() ,durable );
		}
//		 PATCH: we don't need the JMX support and I want to get read of the jmx *.jar files
//		try {
//			if(!serviceName.startsWith(VirtualTopicManager.HIERARCHICAL_TOPIC_DELIMITER)){
//				MantaAgent.getInstance().getSingletonRepository().getMantaJMXManagment().addManagedObject(this, "MantaRay:topic="+this.getServiceName());
//			}
//
//        } catch (MantaException e) {
//            if(log.isErrorEnabled()){
//                log.error("Could not create the JMX MBean.",e);
//            }
//        }
	}


	private String cleanupServiceName(String serviceName){
		String result = serviceName;
		result = StringUtils.replace(result,VirtualTopicManager.HIERARCHICAL_TOPIC_DELIMITER, HIERARCHY_DELIMITER);
		return result;
	}

	/**
	 * Paused the topic. Pausing means producers will still be able to produce
	 * messages, but those messages will be held by the producers and not sent
	 * to the consumers until the topic is resumed again.
	 */
	public synchronized void pause(){
		if(pause ==false){
			PostOffice po= MantaAgent.getInstance().getSingletonRepository()
			.getPostOffice();
			List consumers =  this.getConsumers();
			Iterator consumersIter = consumers.iterator();
			while(consumersIter.hasNext()){
				ServiceConsumer sub = (ServiceConsumer) consumersIter.next();
				PostOfficeBox pob = po.getPostOfficeBox(sub.getId());
				pob.pause();
			}
			pause = true;
		}

	}

	/**
	 * @return true if the queue is paused at the moment, false otherwise
	 */
	public boolean isPaused(){
	    return pause;
	}
	/**
	 * resumes a topic that was paused.
	 */
	public synchronized void resume(){
		if(pause !=false){
			PostOffice po= MantaAgent.getInstance().getSingletonRepository()
			.getPostOffice();
			List consumers =  this.getConsumers();
			Iterator consumersIter = consumers.iterator();
			while(consumersIter.hasNext()){
				ServiceConsumer sub = (ServiceConsumer) consumersIter.next();
				PostOfficeBox pob = po.getPostOfficeBox(sub.getId());
				pob.resume();
			}
			pause = false;
		}

	}

	/**
	 * Purges all the messages currently held by the topic producers. This method
	 * can be operated only after the topic has been paused.
	 */
	public void purge(){
		PostOffice po= MantaAgent.getInstance().getSingletonRepository()
		.getPostOffice();
		List consumers =  this.getConsumers();
		Iterator consumersIter = consumers.iterator();
		while(consumersIter.hasNext()){
			ServiceConsumer sub = (ServiceConsumer) consumersIter.next();
			PostOfficeBox pob = po.getPostOfficeBox(sub.getId());
			pob.purge();
		}
	}



	/**
	 * overload super addConsumer in order to same durable consumer
	 */
	public void addConsumer(ServiceConsumer consumer){

		ServiceConsumer s = (ServiceConsumer)subscribers.get(consumer.getId());
		if (s != null) {
			if (//s.getNoLocal() != consumer.getNoLocal() ||
				!s.getServiceName().equals(consumer.getServiceName()) ||
				!checkEqual(s.getSelectorStatment(), consumer.getSelectorStatment())) {
				subscribers.remove(consumer.getId());
				super.removeConsumer(consumer);
			}
		}
		subscribers.put(consumer.getId(), consumer, consumer.isDurable());
		super.addConsumer(consumer);
	}

	private boolean checkEqual(Object o1, Object o2) {
		if (o1 == null) {
			return o2 == null;
		}
		if (o2 != null) {
			return o1.equals(o2);
		}
		return false;
	}

	/**
	 * overload super removeConsumer in order to same durable consumer
	 */
	public void removeConsumer(ServiceConsumer consumer){
		if(!consumer.isDurable()){
			subscribers.remove(consumer.getId());
			super.removeConsumer(consumer);
		}else{
			ServiceActorControlCenter.removeUpConsumer(consumer);
		}

	}

	/**
	 * @return  byte SERVICE_TYPE_TOPIC
	 */
	public byte getServiceType() {
		return MantaService.SERVICE_TYPE_TOPIC;
	}

	/**
	 * send a message to the topic subscribes
	 * @param message the message to be sent
	 * @param producer the address info of the generator of the message
	 * @param deliveryMode see MantaBusMessageConsts for types
	 * @param priority see MantaBusMessageConsts for types
	 * @param ackType see MantaBusMessageConsts for types
	 * @param expiration after this time the message will not be sent (milli GMT)
	 * @throws IOException
	 */
	public void publish(MantaBusMessage message, ServiceProducer producer, byte deliveryMode, byte priority, long expiration) throws IOException {

		if(log.isDebugEnabled()){
            log.debug("Message arrived. Message ID="+message.getMessageId());
        }

		List currentConsumers = new ArrayList();
		synchronized (this.getConsumers()) {
			currentConsumers.addAll(this.getConsumers());
		}
		int size = currentConsumers.size();
		// if no one is listening to this topic we should not send it
		if(size == 0 ){
			if(log.isDebugEnabled()){
	            log.debug("No consumer found for message "+message.getMessageId()+". The message was not sent");
	        }
			return;
		}
		// prepere the selecotor objects
		boolean sentToConsumer =true;
		SelectorsManager manager = MantaAgent.getInstance().getSingletonRepository().getSelectorsManager();
		String payloadType =message.getHeader(MantaBusMessageConsts.HEADER_NAME_PAYLOAD_TYPE);
		PayLoadSelector select = manager.getSelector(payloadType);

		//	we have multi consumers create the list
		for (int i = 0; i < size; i++) {
			sentToConsumer =true;
			ServiceConsumer consumer = (ServiceConsumer) currentConsumers.get(i);
			if(consumer == null){
				continue;
			}
			// fillter consumers
			if(select!= null){
				//here we look at the payload select a payload selector that will
				// inspect the payload with the consumer select statment
				sentToConsumer =select.accept(consumer.getSelectorStatment() ,message );
			}
			// if ok send to consumer
			if(sentToConsumer){
				MantaBusMessage copy;
				if(size ==1){
					copy = message;
				}else{
					copy =  PostOffice.prepareMessageShallowCopy(message);
				}
				RecipientAddress adderss = consumer;
				copy.setRecipient(adderss);
				copy.addHeader(MantaBusMessageConsts.HEADER_NAME_LOGICAL_DESTINATION ,consumer.getId() );
				// if message or topic are persistent the message is persistent
				if(this.getPersistentMode() == MantaAgentConstants.PERSISTENT)
					deliveryMode = MantaAgentConstants.PERSISTENT;

				if(log.isDebugEnabled()){
		            log.debug("Sending message "+copy.getMessageId()+" to consumer "+consumer.getId());
		        }
				// send to regular send message
				MantaAgent.getInstance().send(copy,producer , deliveryMode , priority,  expiration );
			}
		}


	}//publish



	/**
	 * calls super.removeProducer(producer)
	 * and then check if this topic is no longger needed, if so removes it
	 */
	public void removeProducer(ServiceProducer producer){
		super.removeProducer(producer);
	}


	public void removeDurableConsumer(ServiceConsumer consumer) {

			subscribers.remove(consumer.getId());
			super.removeConsumer(consumer);
			ServiceActorControlCenter.removeUpConsumer(consumer);
			MantaAgent.getInstance().getSingletonRepository().getPostOffice().closeBox(consumer);
	}

}
