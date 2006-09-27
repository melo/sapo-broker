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
 * The Initial Developer of the Original Code is Nimo.
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
package org.mr.api.jms;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mr.MantaException;
import org.mr.core.protocol.MantaBusMessage;
import org.mr.core.protocol.MantaBusMessageConsts;
import org.mr.core.util.SynchronizedPriorityQueue;
import org.mr.core.util.SynchronizedQueue;
import org.mr.core.util.SystemTime;
import org.mr.kernel.services.ServiceConsumer;

import pt.com.manta.ErrorHandler;

/**
 * This class represents a Message consumer object.
 * The domain-specific consumers in the Manta system extend this class.
 * When a user asks for a domain specific consumer, it will not have the
 * methods declared by the other domain.
 * 
 * @version 1.0
 * @since Jan 18, 2004
 * @author Nimo
 *
 */
public class MantaMessageConsumer implements MessageConsumer, QueueReceiver,
		TopicSubscriber, Serializable {

	/**
	 * A constructor for a MessageConsumer object.
	 * 
	 * @param clientID - the clientId that the session gives this consumer.
	 * @param sess - the creating session
	 * @param destination - the queue/topic that this consumer is hooked on.
	 * @param messageSelector - a message selector for messages on the system.
	 * @param noLocal - is this topic local?
	 * @throws JMSException - in case the consumer can not be created.
	 */
	public MantaMessageConsumer(String clientID, MantaSession sess,
			Destination destination, String messageSelector, boolean noLocal,
			ServiceConsumer service) throws JMSException {

		if (sess == null)
			throw new JMSException("MNJMS00060 : CAN NOT CREATE A CONSUMER. SESSION IS NULL.");

		if (messageSelector == null || messageSelector.length()==0)
			messageSelector = null;

		//A new Selector object is created allowing us to check if the message selector constructs a valid
		//selector sentence an InvalidSelectorException is thrown otherwise..

		theMessageSelector = messageSelector;

		isClosed = false;
		theDestination = destination;
		this.clientID = clientID;
		this.creatingSession = sess;
		isNoLocal = noLocal;
		theService = service;
		//innerQueue = new SynchronizedQueue();
		innerQueue = new SynchronizedPriorityQueue(10);
		log = LogFactory.getLog("MantaMessageConsumer");
	}//MantaMessageConsumer

	/** 
	 * Closes the message consumer.
	 *
	 * <P>This call blocks until a <CODE>receive</CODE> or message listener in 
	 * progress has completed. A blocked message consumer <CODE>receive</CODE> 
	 * call returns null when this message consumer is closed.
	 *  
	 * @exception JMSException if the JMS provider fails to close the consumer
	 *                         due to some internal error.
	 */
	public synchronized void close() throws JMSException {

		if (isClosed)
			return;
		
		//nimo - 25-1-2004
		isClosed = true;

		// wake up all listeners and blocked threads
		// Don't see anyone licking this object.
		// Keep this line in the meantime. 
		//notifyAll();

		//tell the creating session to remove this consumer. along with its
		//service.
		if (creatingSession != null)
			creatingSession.removeConsumer(this);
		
		creatingSession = null;
		theMessageSelector = null;
		theService = null;
		clientID = null;
		this.messageListener = null;
	}

	/** 
	 * Gets the message consumer's <CODE>MessageListener</CODE>.
	 *  
	 * @return the listener for the message consumer, or null if no listener
	 * is set
	 *  
	 * @exception JMSException if the JMS provider fails to get the message
	 *                         listener due to some internal error.
	 * @see javax.jms.MessageConsumer#setMessageListener
	 */
	public MessageListener getMessageListener() throws JMSException {
		checkLegalOperation();
		return this.messageListener;
	}//getMessageListener

	/** 
	 * Gets this message consumer's message selector expression.
	 *  
	 * @return this message consumer's message selector, or null if no
	 *         message selector exists for the message consumer (that is, if 
	 *         the message selector was not set or was set to null or the 
	 *         empty string)
	 *  
	 * @exception JMSException if the JMS provider fails to get the message
	 *                         selector due to some internal error.
	 */
	public String getMessageSelector() throws JMSException {
		checkLegalOperation();
		return theMessageSelector;
	}//getMessageSelector

	/** 
	 * Receives the next message produced for this message consumer.
	 *  
	 * <P>This call blocks indefinitely until a message is produced
	 * or until this message consumer is closed.
	 *
	 * <P>If this <CODE>receive</CODE> is done within a transaction, the 
	 * consumer retains the message until the transaction commits.
	 *  
	 * @return the next message produced for this message consumer, or 
	 * null if this message consumer is concurrently closed
	 *  
	 * @exception JMSException if the JMS provider fails to receive the next
	 *                         message due to some internal error.
	 * 
	 */
	public Message receive() throws JMSException {

		return receive(0);
	}//receive

	/** 
	 * Receives the next message that arrives within the specified
	 * timeout interval.
	 *  
	 * <P>This call blocks until a message arrives, the
	 * timeout expires, or this message consumer is closed.
	 * A <CODE>timeout</CODE> of zero never expires, and the call blocks 
	 * indefinitely.
	 *
	 * @param timeout the timeout value (in milliseconds)
	 *
	 * @return the next message produced for this message consumer, or 
	 * null if the timeout expires or this message consumer is concurrently 
	 * closed
	 *
	 * @exception JMSException if the JMS provider fails to receive the next
	 *                         message due to some internal error.
	 */
	public Message receive(long timeout) throws JMSException {
		
		checkLegalOperation();
		if (timeout < 0)
			return null;

		if (timeout == 0)
			timeout = Long.MAX_VALUE;

		if (creatingSession.isStopped) {
			long startTime = System.currentTimeMillis();
			try {
				synchronized (creatingSession.lockMonitor) {
					creatingSession.lockMonitor.wait(timeout);
					//if stopped - wait your timeout (block, as per spec).
				}

			} catch (InterruptedException e) {
				if (log.isErrorEnabled())
					log.error("Error while waiting for the session to resume. ", e);
			}
			//reduce timeout by time passed, more or less. 
			//so - if session was started again-  it will have some more
			//time to receive.
			timeout = timeout - (System.currentTimeMillis() - startTime);
			if (timeout < 1000) //not enough for a receiveNoWait even
				return null;
		}

		
		
		
		boolean ackOrHold = true;
		MantaBusMessage cbm = null;
		MantaMessage jmsMessage = null;
		//if we are talking about a topic here, we need to only talk
		//with our inner queue, i believe. 
		if (this.theDestination instanceof Topic) {

			boolean goOn = true;
			
			// remove all messages with expired TTL
// 			synchronized (innerQueue) {
// 				List list = innerQueue.getUnderlineList();
// 				Iterator it = list.iterator();
// 				while(it.hasNext()) {
// 					MantaBusMessage candidate = (MantaBusMessage) it.next();
// 					if (candidate.getValidUntil()<SystemTime.gmtCurrentTimeMillis()) {
// 						it.remove();
// 					}
// 				}
// 			}
			
			// now find the next message to return.
			// if we use "noLocal" feature that don't pass messages
			// that were sent from the local machine - just ack them.
			while (goOn && timeout >= 0) {
				goOn = false;
				long start = SystemTime.currentTimeMillis();
				cbm = (MantaBusMessage) innerQueue.dequeue(timeout);
				if (cbm != null &&
					cbm.getValidUntil() < SystemTime.gmtCurrentTimeMillis()) {
					goOn = true;
					continue;
				}
				if (cbm != null) {
					jmsMessage = convertToJMSMessage(cbm,creatingSession);
					if (isBreakingNoLocal(cbm, jmsMessage)) {
						goOn = true;
						long now = SystemTime.currentTimeMillis();
						// we need time reduce the time out by the time passed
						timeout = timeout - (now - start);
						ackOrHold=false;
					}
					else
						ackOrHold=true;
						//creatingSession.ackOrHold(cbm);
				}
			}// while

		} else { //not a topic receive at all 
			try {
				//see - if recover and stuff - we may have us a message here.
				
				synchronized (innerQueue) {
					if (innerQueue.size() > 0)
						cbm = (MantaBusMessage) innerQueue.dequeue(timeout);
				}
				// can replace previous code with the following? same result?
				//cbm = (MantaBusMessage) innerQueue.dequeueNoBlock();

				if (cbm == null){
					//no innerQueue
					cbm = creatingSession.receive(this.getService(), timeout);
				}
				else {
					creatingSession.startLocalTransactionIfNeeded();
				}
					
				//receive will ack.
				if (cbm != null) {
					jmsMessage = convertToJMSMessage(cbm, creatingSession);
					if (jmsMessage == null) {
						//creatingSession.ackOrHold(cbm);
						ackOrHold = false;
					}
				}
			} catch (MantaException me) {
				throw new JMSException("MNJMS00062 : METHOD receive() FAILED INTERNALLY. ERROR TEXT : "+me.getMessage());
			}
		}

		if (cbm == null) {
			return null;
		}
	
		jmsMessage.setWriteableState(false);
		if (creatingSession.sessionAcknowledgementMode == Session.CLIENT_ACKNOWLEDGE ||
				creatingSession.getTransacted()) {
			//creatingSession.sessionAcknowledgementMode == Session.SESSION_TRANSACTED) {
				//|| creatingSession.sessionTransactedMode) {
			if (ackOrHold) {
				MantaMessage msg = jmsMessage.makeCopy();
				cbm.setPayload(msg);
			}
		}
		
		//check expiration
		creatingSession.ackOrHold(cbm);
		return jmsMessage;
	

	}//receive

	
	
	private boolean isBreakingNoLocal(MantaBusMessage cbm,
			MantaMessage jmsMessage) throws JMSException {
		
		String connId = jmsMessage.getConnId();
		if (isNoLocal && connId != null) {
			//if the received message was sent from the same connection it was recieved in-
			//just ack it and don't actually pass it the message
			if (connId.equals(creatingSession.owningConnection.getClientID())) {
				creatingSession.ackOrHold(cbm);//cannot just ack here.
				return true;
			}//if
		}//if
		return false;
	}

	/** 
	 * Receives the next message if one is immediately available.
	 *
	 * @return the next message produced for this message consumer, or 
	 * null if one is not available
	 *  
	 * @exception JMSException if the JMS provider fails to receive the next
	 *                         message due to some internal error.
	 * 
	 */
	public Message receiveNoWait() throws JMSException {

		return receive(3000L);

	}//receiveNoWait

	/** 
	 * Sets the message consumer's <CODE>MessageListener</CODE>.
	 * 
	 * <P>Setting the message listener to null is the equivalent of 
	 * unsetting the message listener for the message consumer.
	 *
	 * <P>The effect of calling <CODE>MessageConsumer.setMessageListener</CODE>
	 * while messages are being consumed by an existing listener
	 * or the consumer is being used to consume messages synchronously
	 * is undefined.
	 *  
	 * @param listener the listener to which the messages are to be 
	 *                 delivered
	 *  
	 * @exception JMSException if the JMS provider fails to set the message
	 *                         listener due to some internal error.
	 * @see javax.jms.MessageConsumer#getMessageListener
	 */
	public void setMessageListener(MessageListener listener) throws JMSException {
		checkLegalOperation();
		
		//if we had an earlier listener on a queue - deregister.
		if (this.messageListener != null && theDestination instanceof Queue)
			this.creatingSession.deregisterFromQueue(this);
		
		//Amir - moved from the end of the method
		this.messageListener = listener;
		
		if (listener != null) {
			//new listener is not null - empty the inner queue to it.
			synchronized (innerQueue) {
				MantaBusMessage mbm;
				synchronized(creatingSession.listenersCount) {
					if (creatingSession.isClosed||creatingSession.isClosing)
						return;
					creatingSession.listenersCount.add();
				}
				
				while (!innerQueue.isEmpty()) {
					mbm = (MantaBusMessage) innerQueue.dequeue();
					if (mbm.getValidUntil()>SystemTime.gmtCurrentTimeMillis()) {
						creatingSession.ackOrHold(mbm);
						listener.onMessage(convertToJMSMessage(mbm,	creatingSession));
					}
				}
				
				synchronized(creatingSession.listenersCount) {
					creatingSession.listenersCount.remove();
					if (creatingSession.listenersCount.val()==0) {
						creatingSession.listenersCount.notifyAll();
					}
				}
			
				if (theDestination instanceof Queue)
					creatingSession.listenToQueue(this);
			}
		}
	}

	/*
	 * used by the MantaSession to feed a message to the listener.
	 * this method makes sure to convert the message to a JMS one.
	 */
	void feedMessageListener(MantaBusMessage mbm)
			throws JMSException {
		
		checkLegalOperation();
		MantaMessage message = convertToJMSMessage(mbm, creatingSession);
		if (messageListener != null) {
			String connId = message.getConnId();
			// Topics have a feature that we can ignore messages sent from
			// the local machine. In this case we just ack the message without
			// passing it further. This is the place when we check this condition.
			// isNoLocal has to be true, the destination has to be a topic,
			// and the connection id has to be the same. 
			if (isNoLocal && connId != null	&& this.theDestination instanceof Topic) {
				if (connId.equals(creatingSession.owningConnection.getClientID())) {
					creatingSession.ackMessage(mbm); //only for noLocal got local
					return;
				}//if
			}//if

			if (creatingSession.sessionAcknowledgementMode==Session.CLIENT_ACKNOWLEDGE||
					creatingSession.sessionAcknowledgementMode==Session.SESSION_TRANSACTED) {
				MantaMessage copy = message.makeCopy();
				mbm.setPayload(copy);
			}
			
			// start local transaction if needed
			this.creatingSession.startLocalTransactionIfNeeded();
			
			// lock resources before passing the message further
			creatingSession.ackOrHold(mbm);

			// passing the message to the message listener
			if (this.messageListener != null) {
				try {
					//PATCH: Check if null message
					if(message!=null)
						this.messageListener.onMessage(message);
				} catch (Throwable t) {
					//PATCH: Fail if OOM
					ErrorHandler.checkAbort(t);
					log.error("Exception in message listener: " + t.getMessage());
				}
			}
			else {
				log.error("Message arrived to a consumer with no registered listener. MessageID="+message.getJMSMessageID());
			}
		}
		//else - we are talking about a consumer with no listener.
		else
			innerQueue.enqueue(mbm);
	}

	/*
	 * implementation of queue/topic stuff.
	 */

	/**
	 * Get the queue for this consumer 
	 */
	public Queue getQueue() throws JMSException {
		checkLegalOperation();
		return (Queue) getDestination();
	}

	/**
	 * Gets whether this is a local receiver
	 */
	public boolean getNoLocal() throws JMSException {
		checkLegalOperation();
		return isNoLocal;
	}

	/**
	 * gets the topic for this consumer.
	 */

	public Topic getTopic() throws JMSException {
		checkLegalOperation();
		return (Topic) getDestination();
	}

	private void checkLegalOperation() throws JMSException {
		if (isClosed)
			throw new IllegalStateException("MNJMS00061 : OPERATION UNALLOWED. METHOD FAILED: checkLegalOperation(). REASON : CONSUMER IS CLOSED.");
			
	}

	/*
	 * This method checks to see if the MantaBusMessage is a valid JMS message 
	 * and if so it converts the MantaBusMessage to a JMS message.
	 *  
	 * @param message the MantaBusMessage to be converted
	 * @returnthe newly converted JMS message
	 */
	static MantaMessage convertToJMSMessage(MantaBusMessage message,
			MantaSession session) throws JMSException {
		
		MantaMessage payload = null;
		if (message != null) {
			if (message.getHeader(
					MantaBusMessageConsts.HEADER_NAME_PAYLOAD_TYPE).equals(
					MantaBusMessageConsts.PAYLOAD_TYPE_JMS))
				payload = (MantaMessage) (message.getPayload());

		}//if
		if (payload == null)
			return null;

		MantaMessage result = null;
		if (payload.connId != null) {
			//this messages was genereted in this VM we need to make a copy
			result = payload.makeCopy();
		} else {
			result = payload;
		}

		result.creatingSession=session;
		//the message was just received, so its saved props and body are reset.
		result.flags = result.flags & 0x0FF9FFFF;
		if (result.getJMSType().equals(MantaMessage.BYT_M))   {
			MantaBytesMessage r = (MantaBytesMessage)result;
			r.reset();
		}
		else if (result.getJMSType().equals(MantaMessage.STR_M))
		{	
			MantaStreamMessage r = (MantaStreamMessage)result;
			r.reset();
		}
		
		result.setWriteableState(true);

		//handle recover. and Redelivered
		if (message.isRedelivered())
			result.flags=result.flags|MantaMessage.IS_REDELIVERED;
		
		result.setWriteableState(false);
		return result;
	}//convertToJMSMessage

	Destination getDestination() throws JMSException {
		checkLegalOperation();
		return theDestination;
	}

	String getClientId() {
		return clientID;
	}

	ServiceConsumer getService() {
		return theService;
	}

	//The destination object associated with the consumer
	protected Destination theDestination = null;

	//The message selector object associated with the consumer
	protected String theMessageSelector = null;

	// if true, and the destination is a topic, inhibits the
	// delivery of messages published by its own connection. The
	// behavior for <CODE>NoLocal</CODE> is not specified if the
	// destination is a queue.
	protected boolean isNoLocal;

	// The creating session of this Consumer.  	
	protected MantaSession creatingSession = null;

	//is this consumer closed ?
	boolean isClosed;

	//specific client id for this consumer.
	protected String clientID;

	//The service that this consumer is on - for closing time.
	protected ServiceConsumer theService;

	//the listener
	private MessageListener messageListener = null;

	private SynchronizedQueue innerQueue;
	
	private Log log;
	
}//MantaMessageConsumer