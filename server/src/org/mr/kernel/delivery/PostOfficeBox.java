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
 * Created on 31/05/2004
 *
 * Manta LTD
 */
package org.mr.kernel.delivery;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mr.MantaAgent;
import org.mr.MantaAgentConstants;
import org.mr.core.net.AgentStateListener;
import org.mr.core.net.NetworkManager;
import org.mr.core.persistent.PersistentMap;
import org.mr.core.protocol.DeadEndRecipient;
import org.mr.core.protocol.MantaBusMessage;
import org.mr.core.protocol.MantaBusMessageConsts;
import org.mr.core.protocol.MantaBusMessageUtil;
import org.mr.core.protocol.RecipientAddress;
import org.mr.core.util.PrioritizedList;
import org.mr.core.util.SystemTime;
import org.mr.kernel.services.DeadLetterHandler;
import org.mr.kernel.services.ServiceActorControlCenter;
import org.mr.kernel.services.ServiceConsumer;


/**
 * PostOfficeBox is a hold the outgoing message to a recipient agent
 * and know how to send/resend/dump/save the messages
 * @author Amir Shevat
 *
 */
public class PostOfficeBox implements AgentStateListener {
	NetworkManager manager;
	// the recipient agent name
	RecipientAddress recipient;
	// see NetworkManager
	int agentState = AGENT_STATE_NOT_MONITORING;
	// recipient status
	private boolean recipientOnline = true;

	// the old message mapper but per recipient
	PersistentMap savedMessages;
	// if true the agentName == this agent name and no network is needed
	private boolean localBox = false;
	// if the recipient is durable
	public boolean durable = false;
	// if true the box is monitoring the state of the recipient agent
	private boolean netWorkKeepAliveOn = false;
	// moderator for this recipient
	NetworkModerator moderator;
	// logger
	private Log log;


	private static int DefaultNumberOfMessageInNetBuffer = 100;
	private boolean notYetRecovered = true;

	private boolean pause = false;
	private boolean gotRejects = false;
	private boolean throttle = false;
	private boolean throttling = false;
	private int		highWatermark;
	private int		lowWatermark;
	private long	throttleSleep;
	

	/**
	 * @param agentName the name of the recipient agent
	 */
	public PostOfficeBox(RecipientAddress recipient)  {

		if(recipient instanceof ServiceConsumer && ((ServiceConsumer)recipient).isDurable()){
			durable = true;
			recipientOnline = ServiceActorControlCenter.isConsumerUp(recipient);
		}
		this.recipient = recipient;
		moderator = new NetworkModerator(recipient.getId() , DefaultNumberOfMessageInNetBuffer);
		if(MantaAgent.getInstance().getAgentName().equals(recipient.getAgentName())){
			localBox = true;
			//agentState = AGENT_STATE_UP;
		}

		manager = MantaAgent.getInstance().getSingletonRepository().getNetworkManager();
		log=LogFactory.getLog("PostOfficeBox");

	}

	public PostOfficeBox(RecipientAddress recipient, boolean throttle,
						 int highWatermark, int lowWatermark,
						 long throttleSleep)
	{
		this(recipient);
		this.throttle = throttle;
		this.highWatermark = highWatermark;
		this.lowWatermark = lowWatermark;
		this.throttleSleep = throttleSleep;
		initSavedMessages();
	}

	private synchronized  void  initSavedMessages() {
		if(savedMessages != null)
			return;
			savedMessages = new PersistentMap("messages_to_"+recipient.getId() ,true ,true);
			notYetRecovered = false;
	}

	/**
     * called by the logic or the resending reactor this method sends/saves the message
     * if needed
     * @param msg the message to be sent
     */
    public synchronized void handleMessage(MantaBusMessage msg) {
    	long now = SystemTime.gmtCurrentTimeMillis() ;
		if( (msg.getValidUntil() < now) ){
			// message is old and going to dead letter queue
		    if(log.isDebugEnabled()){
		    	log.info("Not sending message due to TTL expiration: Message ID="+msg.getMessageId()+
		    	         ", Expiration Time="+msg.getValidUntil()+",Current Time="+now+".");
			}
			msg.addHeader(MantaBusMessageConsts.HEADER_NAME_SENT_FAIL , MantaBusMessageConsts.HEADER_VALUE_TRUE);
			DeadLetterHandler.HandleDeadMessage(msg);
		}else{
			// all is good here
			save(msg);
		    if(recipientOnline){
		        sendToNet(msg);
		    }
		}
    }




	private void sendToNet(MantaBusMessage msg){
		if(!pause){
			msg.setRecipient(this.recipient);
			msg.setDeliveryCount(msg.getDeliveryCount()+1);
			if(localBox){
				if(log.isDebugEnabled()){
					log.debug("Sending message "+msg.getMessageId()+" to local recipient.");
				}
				MantaAgent.getInstance().getSingletonRepository().getIncomingMessageManager().messageArrived(msg);
				return;
			}else{
//				 if we are not minotoring the recipient we better start
				if(!netWorkKeepAliveOn){
					manager.addAgentStateListener(msg.getRecipient().getAgentName() , this);
					netWorkKeepAliveOn = true;
				}
//				send to network
			    if(log.isDebugEnabled()){
					log.debug("About to send message "+msg.getMessageId()+".");
				}
				try {
					moderator.sendToNetwork(msg);
				} catch (Exception e) {
				    if(log.isErrorEnabled()){
						log.error("An error occured while trying to send message "+msg.getMessageId()+"." ,e);
					}
				}//try
			}
		}

	}//sendToNet

	/**
	 * saves the message in the map and on disk if needed
	 * @param msg the message to be saved
	 */
	private void save(MantaBusMessage msg){
		//if no ack can be returned by the recipient no need to save
		if ((msg.getRecipient().getAcknowledgeMode() ==
			 MantaAgentConstants.NO_ACK) ||
			msg.isRerouted()) {
			return;
		}
		//if needed save message
		byte delivery = msg.getDeliveryMode();
		boolean persistent = (delivery == MantaAgentConstants.PERSISTENT);
		

		//PATCH: try to avoid deadlock, initialization is best done in the constructor
		//initSavedMessages()

		// we do not keep messages to non durable recipients
		if(!durable){
			persistent = false;
		}

		if(log.isDebugEnabled()){
			log.debug("Ack required. Saving Manta message "+msg.getMessageId()+".");
		}

		synchronized(savedMessages){
			savedMessages.put(msg.getMessageId() , msg ,persistent );
		}

		// if the throttle feature is activated, delay the action of
		// this box if the number of backed up messages have exceeded
		// the high watermark, and have not gone below the low
		// watermark since.
		if (throttle) {
			int backlog = savedMessages.size();
			if (backlog > this.highWatermark) {
				this.throttling = true;
			}
			if (throttling) {
				if (backlog < this.lowWatermark) {
					this.throttling = false;
				} else {
					try {
						Thread.sleep(this.throttleSleep);
					} catch (InterruptedException e) {}
				}
			}
		}
	}

	/**
	 * got ack no need to keep in box anymore if all recipients acked it
	 * @param messageId the message that was aked
	 */
	public MantaBusMessage gotAck(String messageId){
		
		//PATCH: try to avoid deadlocks, initialization is best done in the constructor
		//initSavedMessages()

		MantaBusMessage msg =(MantaBusMessage) savedMessages.get(messageId);

		if(msg != null){
			synchronized (msg) {
				if(log.isDebugEnabled()){
					log.debug("Got ack for message id " + messageId + " from " +
							     msg.getRecipient() +
							     ". Removing message from saved messages list.");
				}

		        	// we do not need this message any more it has been sent
		        	synchronized(savedMessages){
		    			savedMessages.remove(messageId);
		    		}
			}//synchronized
		}// if

		return msg;

	}//gotAck

	public MantaBusMessage gotRejectAck(String messageId) {

		//PATCH: try do avoid deadlocks
		//initSavedMessages();
		MantaBusMessage msg =(MantaBusMessage) savedMessages.get(messageId);

		if(msg != null){
			synchronized (msg) {
				if(durable){
					if(log.isDebugEnabled()){
						log.debug("Got reject ack for message id " + messageId + " from " +
								     msg.getRecipient() +
								     ". This is a durable recipient so we keep the message ");
					}
					this.gotRejects  = true;
				}else{
					if(log.isDebugEnabled()){
						log.debug("Got reject ack for message id " + messageId + " from " +
								     msg.getRecipient() +
								     ". Removing message from saved messages list.");
					}
//					 we do not need this message any more it has been sent- this is not a durable subscriber
		        	synchronized(savedMessages){
		    			savedMessages.remove(messageId);
		    		}
				}



			}//synchronized
		}// if

		return msg;
	}


	// this method helps to detect a change in the durable subscription.
	// if the recovered messages were sent to a durable subscriber with
	// different settings, the messages will be deleted.
	private boolean checkMessageMatchConsumer() {
		synchronized(savedMessages){
			Iterator sentIter = savedMessages.values().iterator();
			MantaBusMessage orig =(MantaBusMessage) sentIter.next();
			ServiceConsumer messageRecipient =  (ServiceConsumer)orig.getRecipient();
			ServiceConsumer boxRecipient = (ServiceConsumer)this.recipient;
			if (//bug:421 messageRecipient.getNoLocal() != boxRecipient.getNoLocal() ||
				!messageRecipient.getServiceName().equals(boxRecipient.getServiceName()) ||
				!checkEqual(messageRecipient.getSelectorStatment(), boxRecipient.getSelectorStatment())) {
				return false;
			}
		}
		return true;
	}

	private boolean checkEqual(Object o1, Object o2) {
        //Aviad changed this method - null selector String is like an empty(i.e "") String
        if (o1 == null) {
            o1 = "";
        }
        if (o2 == null) {
            o2 = "";
        }
        return o1.equals(o2);
//        if (o1 == null) {
//			return o2 == null;
//		}
//		if (o2 != null) {
//			return o1.equals(o2);
//		}
//		return false;
	}

	/**
	 * checks if we need to send or resend messages
	 */
	public synchronized void recoverBox() {
		//PATCH: try do avoid deadlocks
		//initSavedMessages();
		if(savedMessages.isEmpty()){
			return;
		}

		ArrayList tempList = new ArrayList(savedMessages.size());
		synchronized(savedMessages){
			// chek the the durable subscription didn't change.
			// if it changed delete all messages.
			if (durable) {
				if (!checkMessageMatchConsumer()) {
					if (log.isInfoEnabled()) {
						ServiceConsumer boxRecipient = (ServiceConsumer)this.recipient;
						log.info("Durable subscription '"+boxRecipient.getId()+"' was changed. Deleting old subscription's messages.");
					}
					savedMessages.removeAll();
					return;
				}
			}

			// because  the state of the sent messages is not clear,
			// we need to make a copy of them
			Iterator sentIter = savedMessages.values().iterator();
			while(sentIter.hasNext()){
				try {
					MantaBusMessage orig =(MantaBusMessage) sentIter.next();
					if(orig.getDeliveryCount()>0){
						orig = PostOffice.prepareMessageShallowCopy(orig);
					}
					tempList.add(orig);
				} catch (IOException e) {
					log.error("Resending Messages: An error occured during message recovering process.",e);
				}
			}
		}

		// here we order by two orders one time of send and the other is priority
		MantaBusMessageUtil.sortMessagesBySendTime(tempList);
		PrioritizedList resendTempList = new PrioritizedList(MantaAgentConstants.TOTAL_PRIORITIES);
		resendTempList.addAll(tempList);

		// now the list is sorted lets send the messages
		MantaBusMessage msg;
		long now;
		Iterator it = resendTempList.iterator();
		while (it.hasNext()) {
			msg = (MantaBusMessage)it.next();
			now = SystemTime.gmtCurrentTimeMillis() ;
			if (msg.getValidUntil() < now) {
				// message is old and going to dead letter queue
			    if(log.isInfoEnabled()){
			    	log.info("Resending Messages: Not resending message due to TTL expiration: Message ID="+
			    	         msg.getMessageId()+", Expiration Time=" +msg.getValidUntil()+", Current Time="+now+".");
				}
				msg.addHeader(MantaBusMessageConsts.HEADER_NAME_SENT_FAIL , MantaBusMessageConsts.HEADER_VALUE_TRUE);
				synchronized(msg){
					msg.notifyAll();
				}
				savedMessages.remove(msg.getMessageId());
				DeadLetterHandler.HandleDeadMessage(msg);

			}// end of dead old messge
			else{
//				 check if message needs to be sent/resent
				// if the remove agent is down there is nothing for me to do
				// and it is time to send/resend message
			   if(log.isDebugEnabled() && msg.getDeliveryCount() >=1 ){
					log.debug("Resending message "+msg.getMessageId()+".");
				}
				//put current recipient

				// send the message
			   // delivery count is incremented in sendToNet();
			   //msg.setDeliveryCount(msg.getDeliveryCount()+1);
				sendToNet(msg);
			}
			// we have recoved all rejects
			this.gotRejects = false;
		}//for
	}


	/* (non-Javadoc)
	 * @see org.mr.core.net.AgentStateListener#agentStateChanged(java.lang.String, int)
	 */
	public synchronized void agentStateChanged(String agent, int state) {

		if(state == AGENT_STATE_DOWN && agentState != AGENT_STATE_DOWN){
			int oldState = agentState;
			agentState = AGENT_STATE_DOWN;
			// log this only if the agent previously was up
			if(oldState == AGENT_STATE_UP && log.isInfoEnabled()){
				log.info("Got agent down event from peer "+agent+" (box is "+recipient+")");
			}
			setRecipientOnline(false);
		}else if(state == AGENT_STATE_UP && agentState != AGENT_STATE_UP){

			// then allow new messages
			agentState = AGENT_STATE_UP;
			if(!durable || ServiceActorControlCenter.isConsumerUp(recipient)){
				setRecipientOnline(true);
			}
			if(log.isInfoEnabled()){
				log.info("Got agent up event from peer "+agent+" (box is "+recipient+")");
			}
		}

	}

	/**
	 * @return Returns the moderator.
	 */
	public NetworkModerator getModerator() {
		return moderator;
	}

	/**
	 * closes the PO box this is done only when an agent is removed from the world map
	 */
	public void close() {
		manager.removeAgentStateListener(recipient.getAgentName(), this);
		netWorkKeepAliveOn = false;

	}

	public boolean isRecipientOnline() {
		return recipientOnline;
	}

	synchronized void setRecipientOnline(boolean newStatus) {
		if(newStatus == true){
			// recipient online
			if(notYetRecovered == true  ||recipientOnline == false || gotRejects == true ){
				recoverBox();
			}
		}else{
			// recipient offline
			if(recipientOnline == true  ){
				moderator.clear();
			}
			if(recipient instanceof DeadEndRecipient){
				MantaAgent.getInstance().getSingletonRepository().getPostOffice().handleRecipientDown(recipient);
			}
		}

		recipientOnline = newStatus;
	}

	/**
	 * called when the recipient VM went down or when recipient is recalled
	 *
	 */
	void handleRecipientDown(){
		moderator.clear();
		if(durable == false){
			if(savedMessages != null){
				savedMessages.clear();
			}
		}
		if(netWorkKeepAliveOn && !localBox ){
			manager.removeAgentStateListener(recipient.getAgentName() , this);
			netWorkKeepAliveOn = false;
		}
		recipientOnline = false;
	}


	/**
	 * updates the mataray layer of the consumer
	 */
	synchronized void  updateConsumer(ServiceConsumer consumer) {
		if(consumer.isDurable()){
			if(MantaAgent.getInstance().getAgentName().equals(consumer.getAgentName())){
				localBox = true;
			}else{
				localBox = false;
			}
			this.recipient = consumer;
		}
	}

	public synchronized void pause() {
		pause = true;

	}

	public synchronized void resume() {
		pause = false;
		recoverBox();

	}

	public synchronized void purge() {
		savedMessages.clear();

	}

	public HashMap getSavedMessages() {
		HashMap result;
		synchronized(savedMessages){
			 result = new HashMap(savedMessages);
		}
		return result;
	}


}
