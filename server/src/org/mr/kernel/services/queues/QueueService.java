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
package org.mr.kernel.services.queues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mr.MantaAgent;
import org.mr.MantaAgentConstants;
import org.mr.MantaException;
import org.mr.api.jms.MantaConnection;
import org.mr.api.jms.MantaMessage;
import org.mr.api.jms.MantaTextMessage;
import org.mr.core.net.MantaAddress;
import org.mr.core.persistent.PersistentMap;
import org.mr.core.protocol.MantaBusMessage;
import org.mr.core.protocol.MantaBusMessageConsts;
import org.mr.core.protocol.MantaBusMessageUtil;
import org.mr.core.util.PrioritizedList;
import org.mr.core.util.StringUtils;
import org.mr.core.util.SystemTime;
import org.mr.core.util.byteable.ByteableList;
import org.mr.kernel.delivery.DeliveryAckListener;
import org.mr.kernel.delivery.DeliveryAckNotifier;
import org.mr.kernel.delivery.PostOffice;
import org.mr.kernel.services.DeadLetterHandler;
import org.mr.kernel.services.MantaService;
import org.mr.kernel.services.ServiceActorControlCenter;
import org.mr.kernel.services.ServiceActorStatusListener;
import org.mr.kernel.services.ServiceConsumer;
import org.mr.kernel.services.ServiceProducer;
import org.mr.kernel.services.topics.VirtualTopicManager;

/**
 * QueueService is a manta service is a FIFO service with 1 producer and multiple consumers (see jms spec for for info)
 * @since Feb 1, 2004
 * @version  2.0 (1.0 up untill MantaRay 1.6)
 * @author Amir Shevat
 *
 */
 class QueueService extends AbstractQueueService implements DeliveryAckListener, ServiceActorStatusListener, QueueServiceMBean{
	// two strategies in case of overflow
// 	public static final int THROW_EXCEPTION_STRATERGY =0;
// 	public static final int RETURN_WITHOUT_ENQUEUE_STRATERGY =1;
// 	public static final int THROTTLE_STRATERGY =2;
	// time to delay throttled producer
	public static final int throttleDelay = 5;

	protected Log log;
	private PrioritizedList unsentMessages;
	private LinkedList sentMessages;
	private Map savedMessages;
	private QueueSubscriberManager subscriberManager;
	private boolean active;
	private QueueDispatcher dispatch;
//	 a list of receivers waiting on queue data
	private LinkedList queueListeners ;
	/*
	 * The reciver that is currently has unacked messages from this queue.
	 * The queue will send messages only to this QueueReceiver untill all
	 * messages sent to this reciver are acked and then change the currentQueueReceiver
	 */
	private ServiceConsumer currentServiceConsumer = null;
	private Object currentServiceConsumerLock = null;

	private QueueMaster queueMaster;

	private boolean iAmQueueMaster = false;

	private Object queueMasterLockObject = new Object();

	private DeliveryAckNotifier ackNotifier;

	private static long maxQueueSize = Long.MAX_VALUE  ;
    //Aviad - should be only in AbstractQueueService
    //int overflowStrategy;
	// if true messages should not be dequeue form queue
	private boolean pause;
	private Object pauseLockObject = new Object();
	private boolean isTempQueue = false;


	/**
	 * a new queue with a given name
	 * @param serviceName the name of the queue
	 */
	public QueueService(String serviceName ) {
		super(serviceName);
		log=LogFactory.getLog("QueueService");
		isTempQueue =  getServiceName().startsWith(MantaConnection.TMP_DESTINATION_PREFIX);
		subscriberManager = new QueueSubscriberManager(this);
		ackNotifier = MantaAgent.getInstance().getSingletonRepository().getDeliveryAckNotifier();
		maxQueueSize = MantaAgent.getInstance().getSingletonRepository().getConfigManager().getLongProperty("jms.max_queue_size", 1000000);
		overflowStrategy = MantaAgent.getInstance().getSingletonRepository().getConfigManager().getIntProperty("jms.queue_overflow_strategy",2);
//		 PATCH: we don't need the JMX support and I want to get read of the jmx *.jar files
//		if(!isTempQueue){
//			try {
//	            MantaAgent.getInstance().getSingletonRepository().getMantaJMXManagment().addManagedObject(this, "MantaRay:queue="+this.getServiceName());
//	        } catch (MantaException e) {
//	            if(log.isErrorEnabled()){
//	                log.error("Could not create the JMX MBean 'MantaRay:queue="+this.getServiceName()+"'.",e);
//	            }
//	        }
//		}


	}

	/**
	 * @return true if the queue is paused at the moment, false otherwise
	 */
	public boolean isPaused(){
	    return this.pause;
	}

	/**
	 * Paused the queue. Pausing means producers will still be able to produce
	 * messages, but those messages will be held inside the queue and not sent
	 * to the consumers until the queue is resumed again.
	 */
	public void pause(){
		synchronized(pauseLockObject){
			this.pause = true;
		}
	}

	/**
	 * resumes a queue that was paused.
	 */
	public void resume(){
		synchronized(pauseLockObject){
			this.pause = false;
			pauseLockObject.notifyAll();
		}
	}

	/**
	 * Purges all the messages currently held by the queue. This method
	 * can be operated only after the queue has been paused.
	 */
	public void purge(){
		if(unsentMessages != null)
			this.unsentMessages.clear();
		if(sentMessages != null)
			this.sentMessages.clear();
		if(savedMessages != null)
			this.savedMessages.clear();
	}

	/**
	 * closes the queue and stops the reactor thead if it works.
	 * @throws MantaException
	 *
	 */
	public void close() throws MantaException{
		if(dispatch !=null){
			dispatch.stopIt();
		}
		if(iAmQueueMaster){
			MantaAgent.getInstance().recallService(this.getQueueMaster());
			ServiceActorControlCenter.removeConsumerStatusListeners(this);
			synchronized (unsentMessages) {
				Iterator i =  sentMessages.iterator();
				while(i.hasNext()){
					MantaBusMessage msg = (MantaBusMessage) i.next();
					ackNotifier.removeTempListener(msg);
					ackNotifier=null;
				}

			}
		}
		this.active = false;
		purge();

	}

	/**
	 *
	 * @return
	 */
	public List examineMessages(){
		ArrayList list = new ArrayList();
		ByteableList underlineCopy =new ByteableList();
		underlineCopy.addAll(unsentMessages) ;

		int size = underlineCopy.size();
		for (int i = 0; i < size; i++) {
			HashMap details = new HashMap();
			MantaBusMessage msg =(MantaBusMessage) underlineCopy.get(i);
			MantaMessage payload = (MantaMessage) msg.getPayload();
			try {

				String id = payload.getJMSMessageID();
				details.put(QueueServiceMBean.MESSAGE_ID, id);
				if(payload instanceof MantaTextMessage){
					details.put(QueueServiceMBean.MESSAGE_TEXT,((MantaTextMessage)payload).getText());
				}
				HashMap properties = new HashMap();
				Enumeration propNames = payload.getPropertyNames();
				while(propNames.hasMoreElements()){
					String key = (String) propNames.nextElement();
					properties.put(key, payload.getStringProperty(key));
				}
				details.put(QueueServiceMBean.MESSAGE_PROPERTIES, properties);

				HashMap headers = new HashMap();
				headers.put("JMSCorrelationID",String.valueOf(payload.getJMSCorrelationID()));
				headers.put("JMSDestination",String.valueOf(payload.getJMSDestination()));
				headers.put("JMSReplyTo",String.valueOf(payload.getJMSReplyTo()));
				headers.put("JMSType",String.valueOf(payload.getJMSType()));
				headers.put("JMSDeliveryMode",String.valueOf(payload.getJMSDeliveryMode()) );
				headers.put("JMSExpiration",String.valueOf(payload.getJMSExpiration()));
				headers.put("JMSPriority",String.valueOf(payload.getJMSPriority()));
				details.put(QueueServiceMBean.MESSAGE_HEADERS, headers);
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			list.add(details);
		}
		return list;
	}

//Aviad should be only in the AbstractQueueService class
//	//next method added by lital
//	/**
//	 * returns the overFlowStrategy of the Queue
//	 * @return int 0 means THROW_EXCEPTION_STRATERGY,
//	 * 			   1 means RETURN_WITHOUT_ENQUEUE_STRATERGY
//	 * 			   2 means THROTTLING
//	 */
//	public int getOverFlowStrategy(){
//	    return overflowStrategy;
//	}

//	next method added by lital
	/**
	 * @return the max queue size
	 */
	public long getMaxQueueSize(){
	    return maxQueueSize;
	}

	/**
	 * returns type SERVICE_TYPE_QUEUE
	 */
	public byte getServiceType() {
		return super.SERVICE_TYPE_QUEUE;
	}


	private String cleanupServiceName_(String serviceName){
		return  StringUtils.replace(serviceName,VirtualTopicManager.HIERARCHICAL_TOPIC_DELIMITER, "~");
	}

	/**
	 * creates the queue and runs the dispatcher
	 * the dispatcher is the runner thread of this active queue
	 */
	public synchronized void active(){
		if(unsentMessages == null){
			currentServiceConsumerLock = new Object();
			boolean persistent = this.getPersistentMode() ==MantaAgentConstants.PERSISTENT;
			if(!isTempQueue){
				//PATCH: Allow hierarchical names for queues, we need the semantic meaning of the separator.
				savedMessages = new PersistentMap("queueService_"+cleanupServiceName_(this.getServiceName()),persistent,true);
			}else{
				savedMessages = new HashMap();
			}

			unsentMessages = new PrioritizedList(MantaAgentConstants.TOTAL_PRIORITIES);
			sentMessages = new LinkedList();
			queueListeners = new LinkedList();
			active =true;
			ServiceActorControlCenter.addConsumerStatusListeners(this);
			recover();
			dispatch = new QueueDispatcher(this);
			dispatch.start();
		}

	}//active

	/**
	 * returns only when there are listeners in the queue
	 * @throws InterruptedException
	 */
	public void waitForListeners() throws InterruptedException{
		synchronized(queueListeners){
			if(queueListeners.size()>0)
				return;
			queueListeners.wait();
			return;
		}
	}


	/**
	 * register a remote agent to be a receiver of the queue
	 * @param agent the registering agent
	 * @param numberOfReceive the number after witch this receiver will be removed
	 */
	protected void registerReceiverToQueue( ServiceConsumer consumer,long numberOfReceive ){

		QueueReceiver receiver = new QueueReceiver(consumer , numberOfReceive);
		if(numberOfReceive == 0){
			doReceiveNoWait(receiver);

		}else{
			doHandleReceiver(receiver);
		}
		//log.fatal("ADD LISTENER");

	}// registerReceiverToQueue

	private void doHandleReceiver(QueueReceiver receiver){
		synchronized(queueListeners){
			queueListeners.add(receiver);
			queueListeners.notifyAll();
		}
	}//doHandleReceiver

	/**
	 * handles non blocking requests
	 * @param receiver
	 */
	private  void doReceiveNoWait(QueueReceiver receiver){
		MantaBusMessage msg = null;
		if(!pause){
			synchronized(currentServiceConsumerLock){
				if(currentServiceConsumer == null ||currentServiceConsumer.getId().equals(receiver.getConsumer().getId())){
					currentServiceConsumer = receiver.getConsumer();
					synchronized(unsentMessages){
						int size = unsentMessages.size();
						for (int index = 0; index < size; index++) {
							msg =(MantaBusMessage) unsentMessages.get(index);
							if(checkValidMessage(msg ,receiver.getConsumer() )){
								//we found a message for this consumer
								unsentMessages.remove(index);
								sentMessages.addFirst(msg);
								break;
							}else{
								msg = null;
							}
						}
					}
				}
			}
		}//if(!pause)


		if(msg == null ){
			msg = MantaBusMessage.getInstance();
			msg.setPayload(null);
			msg.setSource(this.queueMaster);
			msg.setPriority(MantaAgentConstants.HIGH);
			msg.setDeliveryMode(MantaAgentConstants.NON_PERSISTENT);
			msg.setMessageType(MantaBusMessageConsts.MESSAGE_TYPE_CLIENT);
			msg.addHeader(MantaBusMessageConsts.HEADER_NAME_IS_EMPTY,MantaBusMessageConsts.HEADER_VALUE_TRUE);

		}
		if(ackNotifier != null){
			ackNotifier.setTempListener(msg, this);
		}

		receiver.receive(msg);
	}//doReceiveNoBlock


	/**
	 * removes all the receivers of the Consumer from a queue
	 * @param Consumer - the consumer ID
	 */
	protected  void unregisterConsumerToQueue(ServiceConsumer consumer  ){
		// we need to lockdown the queue for this
		synchronized(unsentMessages){
			synchronized(currentServiceConsumerLock){
				synchronized(queueListeners){
					Iterator receivers = queueListeners.iterator();

					while (receivers.hasNext()) {
						QueueReceiver rec = (QueueReceiver) receivers.next();
						if(rec.getConsumer().getId().equals(consumer.getId())){
							receivers.remove();
							//log.fatal("REMOVED LISTENER - unregister");
						}
					}

					if(currentServiceConsumer != null &&
							currentServiceConsumer.getId().equals(consumer.getId())){
						rollback();
					}


				}
			}
		}
	}// unregisterConsumerToQueue


	public void unregisterReceiverToQueue(ServiceConsumer consumer) {
		synchronized(currentServiceConsumerLock){
			synchronized(queueListeners){
				Iterator receivers = queueListeners.iterator();

				while (receivers.hasNext()) {
					QueueReceiver rec = (QueueReceiver) receivers.next();
					if(rec.getConsumer().getId().equals(consumer.getId())){
						receivers.remove();
						//log.fatal("REMOVED LISTENER - unregister");
					}//if
				}//while

			}
		}

	}//unregisterReceiverToQueue


	private void rollback(){

		// all the messges that are unacked by this reciver
		// are returned to the metrix :)

		// because  the state of the sent messages is not clear,
		// we need to make a copy of them
		LinkedList tempCopy = new LinkedList();
		Iterator sentIter = sentMessages.iterator();
		while(sentIter.hasNext()){
			try {
				MantaBusMessage rollBackMsg = (MantaBusMessage) sentIter.next();
				if(ackNotifier != null){
					ackNotifier.removeTempListener(rollBackMsg);
				}
				tempCopy.add(PostOffice.prepareMessageShallowCopy(rollBackMsg));
			} catch (IOException e) {
				log.error("Rollback error",e);

			}
		}
		unsentMessages.addAllToHead(tempCopy);
		unsentMessages.notifyAll();
		sentMessages.clear();

		synchronized(currentServiceConsumerLock){
			currentServiceConsumerLock.notifyAll();
			currentServiceConsumer = null;

		}
	}

	/**
	 * sends a copy of the queue to a remote agent
	 * @param agentName the receiving agent
	 */
	protected  void sendQueueCopy(ServiceConsumer consumer ) {
		ByteableList underlineCopy =new ByteableList();
		underlineCopy.addAll(unsentMessages) ;

		int size = underlineCopy.size();
		for (int i = 0; i < size; i++) {
			MantaBusMessage msg =(MantaBusMessage) underlineCopy.get(i);
			if(!checkValidMessage(msg ,consumer )){
				// remove the message if not needed
				underlineCopy.remove(i);
				i--;
				size--;
			}
		}

		QueueReceiver receiver = new QueueReceiver(consumer, 0);

		MantaBusMessage msg = MantaBusMessage.getInstance();
		msg.setPayload(underlineCopy);
		msg.setPriority(MantaAgentConstants.HIGH);
		msg.setDeliveryMode(MantaAgentConstants.NON_PERSISTENT);
		msg.setMessageType(MantaBusMessageConsts.MESSAGE_TYPE_CLIENT);

		MantaAddress address = new ServiceProducer(MantaAgent.getInstance().getAgentName(),this.getServiceName(),MantaService.SERVICE_TYPE_QUEUE);
		msg.setSource(address);

		receiver.receive(msg);

	}//sendQueueCopy



	/**
	 * @return Returns true if dispatcher thread has started.
	 */
	public boolean isActive() {
		return active;
	}

	/**
	 * @return Returns the queueMaster.
	 */
	public QueueMaster getQueueMaster() {
		return queueMaster;
	}
	/**
	 * @param queueMaster The queueMaster to set.
	 */
	public void setQueueMaster(QueueMaster queueMaster) {
		QueueMaster oldMaster = null;
		synchronized(queueMasterLockObject){
			if(this.queueMaster!=null){
				oldMaster = this.queueMaster;
			}
			this.queueMaster = queueMaster;
			if(queueMaster != null){
				queueMasterLockObject.notifyAll();
				if(queueMaster.getAgentName().equals(MantaAgent.getInstance().getAgentName())){
					iAmQueueMaster = true;
				}
				subscriberManager.queueCoordinatorFound(queueMaster);
			}else{
				if(iAmQueueMaster){
					iAmQueueMaster = false;
				}else if(isTempQueue){
					try {
						MantaAgent.getInstance().getSingletonRepository().getVirtualQueuesManager().closeQueue(getServiceName());
					} catch (MantaException e) {
						log.error("Failed to close temp queue",e);
					}
				}
			}
			if (oldMaster != null) {
				// kill the po box of the old queue master
				MantaAgent.getInstance().getSingletonRepository().getPostOffice().handleCoordinatorDown(oldMaster, this.queueMaster);
			}

		}
	}

	public boolean amIQueueMaster(){

			return iAmQueueMaster;

	}

	public void waitForQueueMaster(long timeToWait) throws InterruptedException{
		synchronized(queueMasterLockObject){
			if(queueMaster == null){
				queueMasterLockObject.wait(timeToWait);

			}// if
		}// synchronized
	}

	public QueueSubscriberManager getSubscriberManager() {
		return subscriberManager;
	}


	/**
	 * this method dequeues a message and links it with a proper receiver
	 */
	public void doDequeue() throws InterruptedException {
		// if this queue has receivers then dequeue and send to the receiver
		waitForMessages();
		QueueReceiver receiver = findEligibleReceiver();

		if(receiver == null){
			//	check if the consumer that is locking the queue is
			synchronized(unsentMessages){
				synchronized(currentServiceConsumerLock){
					if(currentServiceConsumer !=null){
						if(!ServiceActorControlCenter.isConsumerUp(currentServiceConsumer)){
							rollback();
						}
					}
				}
			}
			Thread.sleep(100);
			return;
		}
		boolean  fed;
		synchronized(unsentMessages){
			fed = feedReceiver(receiver);
		}
		// return the receiver to the end of the list
		if(receiver.getNumberOfReceive()>0){
			synchronized(queueListeners){
				if(ServiceActorControlCenter.isConsumerUp(currentServiceConsumer)){
					queueListeners.addLast(receiver);

				}else{
					// This listener is not currently advertised
					// we need to wait a while and see if he will be re-advertised
					Thread.sleep(1500);
					if(ServiceActorControlCenter.isConsumerUp(currentServiceConsumer)){
						queueListeners.addLast(receiver);
					}//else{
					//	log.fatal("REMOVED LISTENER - consumer down");
					//}

				}
			}
		}//else{
		//	log.fatal("REMOVED LISTENER - consumer done");
		//}

		if(fed){
			synchronized(unsentMessages){
					if(sentMessages.size()>0){
						synchronized(currentServiceConsumerLock){
							currentServiceConsumer = receiver.getConsumer();
						}
					}
			}

		}else{
			// we need to rest and wait
			Thread.sleep(100);
		}
	}//doDequeue

	private void checkNotPause() {
		synchronized(pauseLockObject){
			if(this.pause){
				try {
					pauseLockObject.wait();
				} catch (InterruptedException e) {
					// do nothing
				}
			}
		}


	}

	private QueueReceiver findEligibleReceiver() throws InterruptedException{
		waitForListeners();
		checkNotPause();
		synchronized(currentServiceConsumerLock){

			synchronized(queueListeners){
				if(currentServiceConsumer ==null){
					try{
						return (QueueReceiver) queueListeners.removeFirst();
					}catch(Exception e){
						return null;
					}

				}

				Iterator receivers = queueListeners.iterator();
				while(receivers.hasNext()){
					QueueReceiver receiver = (QueueReceiver) receivers.next();
					if(receiver.getConsumer().getId().equals(currentServiceConsumer.getId())){
						receivers.remove();
						return receiver;
					}
				}
			}//synchronized
		}
		return null;

	}

	private void waitForMessages() throws InterruptedException {
		synchronized(unsentMessages){
			if(unsentMessages.size()>0)
				return;
			unsentMessages.wait();
			return;
		}

	}


	/**
	 * sends 1 message to the receiver while keeping order and selector
	 * @param receiver the reference object to the receiving mantaRay layer
	 * @return true if able to send 1 message to receiver
	 */
	private boolean feedReceiver(QueueReceiver receiver) {
		int size = unsentMessages.size();
		MantaBusMessage msg = null;

		for (int index = 0; index < size; index++) {
			msg =(MantaBusMessage) unsentMessages.get(index);
			// if old message
			if(msg.getValidUntil() <SystemTime.gmtCurrentTimeMillis()){
				if(log.isInfoEnabled()){
					log.info("Not sending message "+msg +"  msg.getValidUntil()=" +msg.getValidUntil()+ " SystemTime.gmtCurrentTimeMillis()=" +SystemTime.gmtCurrentTimeMillis()+".");
				}
				unsentMessages.remove(index);
				DeadLetterHandler.HandleDeadMessage(msg);
				return false;
			}
			if(checkValidMessage(msg ,receiver.getConsumer() )){
				//we found a message for this consumer
				unsentMessages.remove(index);
				sentMessages.addFirst(msg);
				if(ackNotifier!=null)
					ackNotifier.setTempListener(msg, this);
				receiver.receive(msg);
				return true;
			}
		}

		return false;
	}

	protected  void enqueue(MantaBusMessage enqueuedMessage, boolean persistent) {
		synchronized(unsentMessages){
			if(!isTempQueue){
				((PersistentMap)savedMessages).put(enqueuedMessage.getMessageId(),enqueuedMessage, persistent);
			}else{
				savedMessages.put(enqueuedMessage.getMessageId(),enqueuedMessage);
			}

			unsentMessages.add(enqueuedMessage);
			unsentMessages.notifyAll();
		}
	}

	/**
	 * checks if size of queue is too big
	 * @return true if invoking method should return without enqueueing
	 */
	public boolean isOverflow() {
		int size = unsentMessages.size();
		if(size < maxQueueSize ){
			return false;
		}
		return true;

	}

	/**
	 * remove the acked message and release the current Receiver so others can share
	 */
	public void gotAck(MantaBusMessage msg, MantaAddress source) {
		synchronized(unsentMessages){
			savedMessages.remove(msg.getMessageId());
			sentMessages.remove(msg);
			if(sentMessages.size() == 0){
				synchronized( currentServiceConsumerLock){
					if(currentServiceConsumer!= null  ){
						currentServiceConsumerLock.notifyAll();
						currentServiceConsumer = null;
					}
				}
			}
		}
	}//gotAck

	 /**
	  * re-queue the acked message and release the current receiver so
	  * others can share
	  */
	 public void gotAckReject(MantaBusMessage msg, MantaAddress source) {
		 synchronized(unsentMessages) {
			 msg.setDeliveryCount(msg.getDeliveryCount()+1);
			 unsentMessages.addToHead(msg);
			 sentMessages.remove(msg);
			 if (sentMessages.size() == 0) {
				 synchronized (currentServiceConsumerLock) {
					 if (currentServiceConsumer!= null) {
						 currentServiceConsumerLock.notifyAll();
						 currentServiceConsumer = null;
					 }
				 }
			 }
		 }
	 }

	/**
	 * checks if we need to send or resend messages
	 */
	public synchronized void recover() {

		if(savedMessages.isEmpty()){
			return;
		}
		ArrayList tempList = new ArrayList();
		synchronized(savedMessages){
			tempList.addAll(savedMessages.values());
		}
        //Aviad use another sort method - to sort by enqueue time and not send time
        MantaBusMessageUtil.sortMessagesByEnqueueTime(tempList,VirtualQueuesManager.ENQUEUE_TIME);
        //MantaBusMessageUtil.sortMessagesBySendTime(tempList);

		int size = tempList.size();
		for (int i = 0; i < size; i++) {
			MantaBusMessage msg = (MantaBusMessage)tempList.get(i);
			long now = SystemTime.gmtCurrentTimeMillis() ;
			if( (msg.getValidUntil() <now ) ){
				if(log.isInfoEnabled()){
					log.info("Not sending message "+msg +"  msg.getValidUntil()=" +msg.getValidUntil()+ " SystemTime.gmtCurrentTimeMillis()=" +SystemTime.gmtCurrentTimeMillis()+".");
				}
				savedMessages.remove(msg.getMessageId());
				DeadLetterHandler.HandleDeadMessage(msg);

			}// end of dead old messge
			else

			// send the message
			unsentMessages.add(msg);

		}//for

    }//resendIfNeeded


	public void handleConsumerUp(ServiceConsumer consumer) {
		// nothing to do

	}


	public void handleConsumerDown(ServiceConsumer consumer) {
		unregisterConsumerToQueue(consumer);

	}



	public int getUnsentCount() {
		return unsentMessages.size();
	}



	public String toString(){
		StringBuffer buff = new StringBuffer();
		buff.append(" service{");
		buff.append(" service name=");
		buff.append(logicalName);
		buff.append(" serviceType=");
		buff.append(getServiceType());
		buff.append(" consumers=");
		buff.append(consumers);
		buff.append(" producers=");
		buff.append(producers);
		buff.append(" coordinator=");
		buff.append(queueMaster);
		buff.append(" persistentMode=");
		buff.append(super.getPersistentMode());
		buff.append(" }");
		return buff.toString();
	}


	public boolean isTempQueue() {
		return isTempQueue;
	}
}
