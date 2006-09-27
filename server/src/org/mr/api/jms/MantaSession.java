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
 * The Initial Developer of the Original Code is Nimo 24-FEB-2004.
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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mr.IMessageListener;
import org.mr.MantaAgent;
import org.mr.MantaException;
import org.mr.api.jms.selector.syntax.Selector;
import org.mr.core.protocol.MantaBusMessage;
import org.mr.core.protocol.MantaBusMessageConsts;
import org.mr.core.util.Stage;
import org.mr.core.util.StageHandler;
import org.mr.core.util.StageParams;
import org.mr.core.util.SystemTime;
import org.mr.core.util.byteable.Byteable;
import org.mr.core.util.byteable.ByteableInputStream;
import org.mr.core.util.byteable.ByteableOutputStream;
import org.mr.core.util.byteable.ByteableRegistry;
import org.mr.kernel.security.MantaAuthorization;
import org.mr.kernel.security.SecurityActionTypes;
import org.mr.kernel.services.MantaService;
import org.mr.kernel.services.ServiceActor;
import org.mr.kernel.services.ServiceConsumer;
import org.mr.kernel.services.ServiceProducer;
import org.mr.kernel.services.topics.VirtualTopicManager;


/**
 *
 * @author Nimo 24-FEB-2004
 *
 * A Session object is a single-threaded context for producing and consuming messages. Although it may allocate
 * provider resources outside the Java virtual machine (JVM), it is considered a lightweight JMS object.
 *
 *
 * A session serves several purposes:
 * It is a factory for its message producers and consumers.
 * It supplies provider-optimized message factories.
 * It is a factory for TemporaryTopics and TemporaryQueues.
 * It provides a way to create Queue or Topic objects for those clients that need to dynamically manipulate provider-specific destination names.
 * It supports a single series of transactions that combine work spanning its producers and consumers into atomic units.
 * It defines a serial order for the messages it consumes and the messages it produces.
 * It retains messages it consumes until they have been acknowledged.
 * It serializes execution of message listeners registered with its message consumers.
 * It is a factory for QueueBrowsers.
 * A session can create and service multiple message producers and consumers.
 * One typical use is to have a thread block on a synchronous MessageConsumer until a message arrives. The thread may then use one or more of the Session's MessageProducers.
 * If a client desires to have one thread produce messages while others consume them, the client should use a separate session for its producing thread.
 * Once a connection has been started, any session with one or more registered message listeners is dedicated to the thread of control that delivers messages to it. It is erroneous for client code to use this session or any of its constituent objects from another thread of control. The only exception to this rule is the use of the session or connection close method.
 * It should be easy for most clients to partition their work naturally into sessions. This model allows clients to start simply and incrementally add message processing complexity as their need for concurrency grows.
 * The close method is the only session method that can be called while some other session method is being executed in another thread.
 * A session may be specified as transacted. Each transacted session supports a single series of transactions. Each transaction groups a set of message sends and a set of message receives into an atomic unit of work. In effect, transactions organize a session's input message stream and output message stream into series of atomic units. When a transaction commits, its atomic unit of input is acknowledged and its associated atomic unit of output is sent. If a transaction rollback is done, the transaction's sent messages are destroyed and the session's input is automatically recovered.
 * The content of a transaction's input and output units is simply those messages that have been produced and consumed within the session's current transaction.
 * A transaction is completed using either its session's commit method or its session's rollback method. The completion of a session's current transaction automatically begins the next. The result is that a transacted session always has a current transaction within which its work is done.
 * The Java Transaction Service (JTS) or some other transaction monitor may be used to combine a session's transaction with transactions on other resources (databases, other JMS sessions, etc.). Since Java distributed transactions are controlled via the Java Transaction API (JTA), use of the session's commit and rollback methods in this context is prohibited.
 * The JMS API does not require support for JTA; however, it does define how a provider supplies this support.
 * Although it is also possible for a JMS client to handle distributed transactions directly, it is unlikely that many JMS clients will do this. Support for JTA in the JMS API is targeted at systems vendors who will be integrating the JMS API into their application server products.
 *
 */
public class MantaSession implements Serializable, Session, QueueSession, TopicSession, IMessageListener,StageHandler {


	/**
	 * in order to be a good Serializable
	 */
	private static final long serialVersionUID = -1698529734026002731L;
	public Log log;
	/**
	 * the fasade of the manta
	 */
	private MantaAgent manta;
	protected Counter listenersCount;

	// When the session is stopped, this object is enqueued to the
	// stage's queue to release the Execution Thread in case the
	// queue is empty.
	private Object stopEvent = new Object();

	// support in JCA 1.5
	private DeliveryListener deliveryListener;

	protected TransactionContext transactionContext;

	// used by the connection consumer only
	private IMessageListener busListener = null;

	//constructors --------------------------------------------------------

   /**
    * The main constructor for a Session object.
    * This constructor needs to have a session id, supplied by the connection,
    * along with the creating connection reference.
    * Additionally - transaction and ack mode for the session.
    */
	MantaSession(String csessId, MantaConnection con, int ackMode, boolean trx) throws JMSException{

		if (con == null)
			throw new JMSException("MNJMS00072 : FAILED ON SESSION CREATION. Connection WAS NULL.");
		owningConnection = con;
		sessId = csessId;
		//sessionTransactedMode=trx;
		if (trx)
			sessionAcknowledgementMode=Session.SESSION_TRANSACTED;
		//check ack_mode in the range Session_Transacted=0, DUPS_OK = 3.
		else if (ackMode<Session.SESSION_TRANSACTED || ackMode>Session.DUPS_OK_ACKNOWLEDGE)
				throw new IllegalStateException("MNJMS00073 : FAILED ON SESSION CREATION. INAVLID ACKNOWLEDGE MODE : "+ackMode);
		else
			sessionAcknowledgementMode=ackMode;

		// we don't know now if this session will participate in XA transactions so
		// we create the lists anyway.
		//if (sessionAcknowledgementMode == Session.SESSION_TRANSACTED ||
		//	sessionAcknowledgementMode == Session.CLIENT_ACKNOWLEDGE) {
			unackedMessages = new LinkedHashSet();
			heldMessages = new LinkedHashSet();
		//}
		//transactionContext = new TransactionContext(this);
		transactionContext = new TransactionContext();
		transactionContext.addSession(this);
//		transactionContext.setLocalTransactionEventListener(new LocalTransactionEventListener() {
//			public void beginEvent(String txnID) {
//				System.out.println("local txn started: "+txnID);
//			}
//			public void commitEvent(String txnID) {
//				System.out.println("local txn commited: "+txnID);
//			}
//			public void rollbackEvent(String txnID) {
//				System.out.println("local txn rolledback: "+txnID);
//			}
//		});

		isClosed = false;
		isClosing = false;
		isStopped = !con.isStarted;
		log=LogFactory.getLog("MantaSession");

		//create the parms for the staging mechanism and the inner stage.
		StageParams innerQueueParms = new StageParams();
		innerQueueParms.setBlocking(false);
		innerQueueParms.setHandler(this);
		innerQueueParms.setMaxNumberOfThreads(1);
		innerQueueParms.setNumberOfStartThreads(1);
		innerQueueParms.setPersistent(false);
		innerQueueParms.setStageName("Session["+sessId+"]@");
		innerQueueParms.setStagePriority(0);
		innerQueue = new Stage(innerQueueParms);
		lockMonitor = new Object();
		manta = owningConnection.getChannel();
		consumerMessages = new ArrayList();
		listenersCount = new Counter();
	}//MantaSession


	/**
	 * This method is used by the connection to start a session.
	 * when a connection is started, a session should know that it can
	 * start too, and notify its consumers who might be waiting.
	 *
	 * @throws JMSException
	 */
	void start() throws JMSException
	{
		checkLegalOperation();
		if (!isStopped)
			return;
		isStopped = false;

		synchronized (lockMonitor) {
			lockMonitor.notifyAll();
		}
	}

	/**
	 * This method is used by the connection to stop all its sessions upon
	 * stopping on itself. this is done so the sessions wouldn't try to do
	 * work when the connection is not in an open state.
	 *
	 * @throws JMSException
	 */
	void stop() throws JMSException
	{
		checkLegalOperation();
		if (isStopped)
			return;

		// in order to make this method blocking, we wait on the
		// lockMonitor object. When the Execution Thread finds
		// out that the session stopped it notifies about it going
		// to sleep and the stop thread resumes and return.
		synchronized (lockMonitor) {
			// we start synchronizing here because we don't want the
			// Execution Thread to notify, before we invoke the wait().
			isStopped = true;

			// When the satage queue is empty, it blocks the Execution
			// Thread. To prevent that we enqueue a demi object.
			innerQueue.enqueue(stopEvent);
			try {
				lockMonitor.wait();
			} catch (InterruptedException e) {
				//PATCH: Don't swallow interrupts
				Thread.currentThread().interrupt();
				if (log.isErrorEnabled())
					log.error("Error while stopping the session. ", e);
			}
		}
	}

	/**
	 * Creates a new <CODE>BytesMessage</CODE> object.
	 * @exception JMSException
	 *                if the JMS provider fails to create this message due to
	 *                some internal error.
	 */
	public BytesMessage createBytesMessage() throws JMSException {
		checkLegalOperation();
		return new MantaBytesMessage(this);
	}//createBytesMessage

	/**
	 * Creates a new <CODE>MapMessage</CODE> object.
	 * @exception JMSException
	 *                if the JMS provider fails to create this message due to
	 *                some internal error.
	 */
	public MapMessage createMapMessage() throws JMSException {
		checkLegalOperation();
		return new MantaMapMessage(this);
	}//createMapMessage

	/**
	 * Creates a <CODE>Message</CODE> object.
	 * @exception JMSException
	 *                if the JMS provider fails to create this message due to
	 *                some internal error.
	 */
	public Message createMessage() throws JMSException {
		checkLegalOperation();
		return new MantaMessage(this);
	}//createMessage

	/**
	 * Creates an <CODE>ObjectMessage</CODE> object.
	 * @exception JMSException
	 *                if the JMS provider fails to create this message due to
	 *                some internal error.
	 */
	public ObjectMessage createObjectMessage() throws JMSException {
		checkLegalOperation();
		return new MantaObjectMessage(this);
	}//createObjectMessage

	/**
	 * Creates an initialized <CODE>ObjectMessage</CODE> object.
	 *
	 * @param object
	 *            the object to use to initialize this message
	 *
	 * @exception JMSException
	 *                if the JMS provider fails to create this message due to
	 *                some internal error.
	 */
	public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
		checkLegalOperation();
		return new MantaObjectMessage(this,object);
	}//createObjectMessage

	/**
	 * Creates a <CODE>StreamMessage</CODE> object.
	 * @exception JMSException
	 *                if the JMS provider fails to create this message due to
	 *                some internal error.
	 */
	public StreamMessage createStreamMessage() throws JMSException {
		checkLegalOperation();
		return new MantaStreamMessage(this);
	}//createStreamMessage

	/**
	 * Creates a <CODE>TextMessage</CODE> object.
	 * @exception JMSException
	 *                if the JMS provider fails to create this message due to
	 *                some internal error.
	 */
	public TextMessage createTextMessage() throws JMSException {
		checkLegalOperation();
		return new MantaTextMessage(this);
	}//createTextMessage

	/**
	 * Creates an initialized <CODE>TextMessage</CODE> object.
	 * @param text
	 *            the string used to initialize this message
	 *
	 * @exception JMSException
	 *                if the JMS provider fails to create this message due to
	 *                some internal error.
	 */
	public TextMessage createTextMessage(String text) throws JMSException {
		checkLegalOperation();
		return new MantaTextMessage(this,text);
	}//createTextMessage

	/**
	 * Indicates whether the session is in transacted mode.
	 * @return true if the session is in transacted mode
	 * @exception JMSException
	 *                if the JMS provider fails to return the transaction mode
	 *                due to some internal error.
	 */
	public final boolean getTransacted() throws JMSException {

		checkLegalOperation();
		//return sessionTransactedMode;
		return sessionAcknowledgementMode == Session.SESSION_TRANSACTED ||
		                                     transactionContext.isInXATransaction();

	}//getTransacted


	/**
	 * Returns the acknowledgement mode of the session. The acknowledgement
	 * mode is set at the time that the session is created. If the session is
	 * transacted, the acknowledgement mode is ignored.
	 *
	 * @return If the session is not transacted, returns the current
	 *         acknowledgement mode for the session. If the session is
	 *         transacted, returns SESSION_TRANSACTED.
	 *
	 * @exception JMSException
	 *                if the JMS provider fails to return the acknowledgment
	 *                mode due to some internal error.
	 *
	 * @see MantaConnection#createSession
	 * @since 1.1
	 */
	public final int getAcknowledgeMode() throws JMSException {

		checkLegalOperation();
		return sessionAcknowledgementMode;
	}//getAcknowledgeMode

	/**
	 * Commits all messages done in this transaction and releases any locks
	 * currently held.
	 *
	 * @exception JMSException
	 *                if the JMS provider fails to commit the transaction due
	 *                to some internal error.
	 * @exception TransactionRolledBackException
	 *                if the transaction is rolled back due to some internal
	 *                error during commit.
	 * @exception IllegalStateException
	 *                if the method is not called by a transacted session.
	 */
	public void commit() throws JMSException {
//		checkLegalOperation();
//		if (!sessionTransactedMode)
//			throw new IllegalStateException("MNJMS00074 : FAILED ON METHOD commit(). SESSION IS NOT TRANSACTED.");
		transactionContext.commit();
	}

	/**
	 * callback from the TransactionContext to do the actual commit
	 */
	protected void commitSession() throws JMSException {
		//ack all received messages.
		synchronized (unackedMessages) {
			Iterator ackIterator = unackedMessages.iterator();
			int size = unackedMessages.size();
			for (int i = 0;i<size;i++) {
				MantaBusMessage mbm = (MantaBusMessage)ackIterator.next();
				owningConnection.ack(mbm);
			}
			unackedMessages.clear();
		}
		//send all messages and clear
		sendAllMessages(heldMessages);
		heldMessages.clear();
	}//commit

	/**
	 * Rolls back any messages done in this transaction and releases any locks
	 * currently held.
	 *
	 * @exception JMSException
	 *                if the JMS provider fails to roll back the transaction
	 *                due to some internal error.
	 * @exception IllegalStateException
	 *                if the method is not called by a transacted session.
	 *
	 */
	public void rollback() throws JMSException {
//		checkLegalOperation();
//		if (!sessionTransactedMode)
//			throw new IllegalStateException("MNJMS00075 : FAILED ON METHOD rollback(). SESSION IS NOT TRANSACTED.");
		transactionContext.rollback();
	}//rollback


	/**
	 * callback from the TransactionContext to do the actual rollback
	 */
	protected void rollbackSession() throws JMSException {
		heldMessages.clear();

		synchronized(listenersCount) {
			// wait for all listeners to finish dealing with messages
			while (listenersCount.val() != 0) {
				try {
					listenersCount.wait();
				}
				catch (InterruptedException ie) {
					
					//PATCH: Don't swallow interrupts
					Thread.currentThread().interrupt();
				}
			}

			// redeliver all unacked messages to the consumers.
			// mark all resent messages as redelivered.
			sendUnackedMessages();
		}
	}//rollbackSession

	/**
	 * Delivers all unacked messages to the consumers.
	 */
	protected void sendUnackedMessages() throws JMSException {
		MantaBusMessage mbm;
		String consumer;
		MantaMessageConsumer destConsumer;
		synchronized(lockMonitor) {
			List unackedMessagesCopy = new ArrayList();
			synchronized(unackedMessages) {
				unackedMessagesCopy.addAll(unackedMessages);
				unackedMessages.clear();
			}
			Iterator unacked = unackedMessagesCopy.iterator();
			while (unacked.hasNext()) {
				mbm = (MantaBusMessage) unacked.next();
				consumer = ((ServiceActor)mbm.getRecipient()).getId();
				destConsumer = (MantaMessageConsumer)messageConsumers.get(consumer);
				if (destConsumer != null) {
					synchronized (destConsumer) {
						if (!destConsumer.isClosed) {
							MantaMessage result = (MantaMessage) mbm.getPayload();
							result.flags = result.flags | MantaMessage.IS_REDELIVERED;
							destConsumer.feedMessageListener(mbm);
						}
					}
				}
				else {
					if (log.isInfoEnabled())
						log.info("A message cannot be sent to a closed consumer. Returning to wait.");
					unackedMessages.add(mbm);
				}
			}
		}
	}//sendUnackedMessages


	void startLocalTransactionIfNeeded() throws JMSException {
		if (sessionAcknowledgementMode == Session.SESSION_TRANSACTED &&
			!transactionContext.isInLocalTransaction() &&
			!transactionContext.isInXATransaction()) {
			transactionContext.begin();
		}
	}


	/**
	 * Closes the session.
	 *
	 * This call will block until a <CODE>receive</CODE> call or message
	 * listener in progress has completed. A blocked message consumer <CODE>
	 * receive</CODE> call returns <CODE>null</CODE> when this session is
	 * closed.
	 *
	 * <P>
	 * Closing a transacted session must roll back the transaction in progress.
	 *
	 * <P>
	 * This method is the only <CODE>MantaSession</CODE> method that is allowed
	 * to come from a different control thread. hence - it is synchronized.
	 *
	 * <P>
	 * Invoking any other <CODE>MantaSession</CODE> method on a closed
	 * session must throw a <CODE>JMSException.IllegalStateException</CODE>.
	 * Closing a closed session must <I>not</I> throw an exception.
	 *
	 * @exception JMSException
	 *                if the JMS provider fails to close the session due to
	 *                some internal error.
	 */
	public synchronized void close() throws JMSException {

		if (isClosed || isClosing)
			return;

		stop();

		// shai: we must rollback the session if it is transacted.
		// However it's illegal to call it on XASession.
		// Another way to do it is to override this method in the
		// MantaXASession and remove this call. But then every change
		// in this code will need to be copied to the MantaXASession
		// class too. mmm.
		//if (sessionTransactedMode && !(this instanceof MantaXASession))
		if (transactionContext.isInLocalTransaction() && !(this instanceof MantaXASession))
			rollback();

		isClosing = true;

		// close all producers
		synchronized(messageProducers) {
			List l = new ArrayList(messageProducers.size());
			l.addAll(messageProducers.values());
			Iterator producers = l.iterator();
			MantaMessageProducer mmp;
			while (producers.hasNext()) {
				mmp = (MantaMessageProducer)producers.next();
				if (mmp != null) {
					mmp.close();
				}
			}
			// log producers that stayed opened
			if (messageProducers.size() > 0) {
				reportRemainingToLog(messageProducers, "producers");
			}
			messageProducers.clear();
		}

		// close all consumers
		synchronized(messageConsumers) {
			List l = new ArrayList(messageConsumers.size());
			l.addAll(messageConsumers.values());
			Iterator consumers = l.iterator();
			MantaMessageConsumer mmc;
			while (consumers.hasNext()) {
				mmc = (MantaMessageConsumer)consumers.next();
				if (mmc != null) {
					mmc.close();
				}
			}
			// log consumers that stayed opened
			if (messageConsumers.size() > 0) {
				reportRemainingToLog(messageConsumers, "consumers");
			}
			messageConsumers.clear();
		}

		busListener = null;

		owningConnection.deleteSession(this);
		owningConnection = null;

//		if (transactionContext.isInXATransaction() || transactionContext.isInLocalTransaction()) {
//			transactionContext.saveTransactionNoPersist(heldMessages, unackedMessages);
//		}

		if (heldMessages != null) {
			heldMessages.clear();
		}

		//mark the session as closed:
		isClosed = true;
		isClosing = false;
		innerQueue.stop();

		synchronized (lockMonitor) {
			lockMonitor.notifyAll();
		}
		//no consumers are on right now, so all types are allowed.
	}

	//txn moved from MantaXASession due to RA requirements
	void saveMessages(MantaXADescriptor descriptor) {
		descriptor.addHeldMessages(heldMessages);
		heldMessages.clear();
		synchronized (unackedMessages) {
			descriptor.addUnackedMessages(unackedMessages);
			unackedMessages.clear();
		}
	}

	// used to write to the log any producers/consumers the remained
	// after session close.
	private void reportRemainingToLog(Hashtable table, String listName) {
		if (log.isInfoEnabled()) {
			String delimiter = ", ";
			StringBuffer buf = new StringBuffer();
			Iterator i = table.values().iterator();
			while (i.hasNext()) {
				buf.append(i.next());
				buf.append(delimiter);
			}
			log.info("Some "+listName+" remained after closing the session. These "+listName+" will be removed: "+buf.toString());
		}
	}

	/**
	 * Stops message delivery in this session, and restarts message delivery
	 * with the oldest unacknowledged message.
	 *
	 * <P>
	 * All consumers deliver messages in a serial order. Acknowledging a
	 * received message automatically acknowledges all messages that have been
	 * delivered to the client.
	 *
	 * <P>
	 * Restarting a session causes it to take the following actions:
	 *
	 * <UL>
	 * <LI>Stop message delivery
	 * <LI>Mark all messages that might have been delivered but not
	 * acknowledged as "redelivered"
	 * <LI>Restart the delivery sequence including all unacknowledged messages
	 * that had been previously delivered. Redelivered messages do not have to
	 * be delivered in exactly their original delivery order.
	 * </UL>
	 *
	 * @exception JMSException
	 *                if the JMS provider fails to stop and restart message
	 *                delivery due to some internal error.
	 * @exception IllegalStateException
	 *                if the method is called by a transacted session.
	 */
	public void recover() throws JMSException {

		checkLegalOperation();
		//if (sessionTransactedMode)
		if (this.getTransacted())
			throw new IllegalStateException("MNJMS00078 : FAILED ON METHOD recover(). SESSION IS TRANSACTED.");

		if (sessionAcknowledgementMode != Session.CLIENT_ACKNOWLEDGE)
			return;

		sendUnackedMessages();
	}//recover


	// used by the MantaConnectionConsumer to bypass a message consumer
	// because it needs to get messges as MantaBusMessages
	void setBusMessageListener(IMessageListener listener) {
		busListener = listener;
	}


	/**
	 * This is for application servers. they use connection consumers,
	 * and therefore will want to use this and the run() method.
	 */
	public void run() {
		synchronized (consumerMessages) {
			Message message;
			MantaBusMessage busMessage;
			while (!consumerMessages.isEmpty()) {
				busMessage = (MantaBusMessage) consumerMessages.remove(0);
				try {
					message = MantaMessageConsumer.convertToJMSMessage(busMessage, this);
					ackOrHold(busMessage);
				} catch (JMSException e) {
					e.printStackTrace();
					busMessage = null;
					message = null;
					continue;
				}


				// notify the application server before sending
				// a message to the MDB (JCA 1.5)
				if (deliveryListener != null)
	                deliveryListener.beforeDelivery(this, message);

				// start local transaction if needed
				try {
					this.startLocalTransactionIfNeeded();
				} catch (JMSException e) {
					e.printStackTrace();
				}

				// send the message
				this.sessionListener.onMessage(message);

				// notify the application server after sending
				// a message to the MDB (JCA 1.5)
				if (deliveryListener != null)
	                deliveryListener.afterDelivery(this, message);
			}
		}
	}



	/**
	 * Returns the session's distinguished message listener (optional).
	 *
	 * @return the message listener associated with this session
	 *
	 * @exception JMSException
	 *                if the JMS provider fails to get the message listener due
	 *                to an internal error.
	 *
	 * @see javax.jms.MantaSession#setMessageListener
	 * @see javax.jms.ServerSessionPool
	 * @see javax.jms.ServerSession
	 */
	public MessageListener getMessageListener() throws JMSException {

		checkLegalOperation();
		return this.sessionListener;
	}//getMessageListener

	/**
	 * Sets the session's distinguished message listener (optional).
	 *
	 * <P>
	 * When the distinguished message listener is set, no other form of message
	 * receipt in the session can be used; however, all forms of sending
	 * messages are still supported.
	 *
	 * <P>
	 * This is an expert facility not used by regular JMS clients.
	 *
	 * @param listener
	 *            the message listener to associate with this session
	 *
	 * @exception JMSException
	 *                if the JMS provider fails to set the message listener due
	 *                to an internal error.
	 *
	 * @see javax.jms.MantaSession#getMessageListener
	 * @see javax.jms.ServerSessionPool
	 * @see javax.jms.ServerSession
	 */
	public final void setMessageListener(MessageListener listener) throws JMSException {

		checkLegalOperation();
		sessionListener = listener;

	}//setMessageListener



	/**
	 * Creates a <CODE>MantaMessageProducer</CODE> to send messages to the
	 * specified destination.
	 *
	 * <P>
	 * A client uses a <CODE>MantaMessageProducer</CODE> object to send
	 * messages to a destination. Since <CODE>MantaQueue</CODE> and <CODE>
	 * MantaTopic</CODE> both inherit from <CODE>Destination</CODE>, they
	 * can be used in the destination parameter to create a <CODE>
	 * MantaMessageProducer</CODE> object.
	 *
	 * @param destination
	 *            the <CODE>Destination</CODE> to send to, or null if this is
	 *            a producer which does not have a specified destination.
	 *
	 * @exception JMSException
	 *                if the session fails to create a MantaMessageProducer
	 *                due to some internal error.
	 * @exception InvalidDestinationException
	 *                if an invalid destination is specified.
	 *
	 * @since 1.1
	 *
	 */
	public MessageProducer createProducer(Destination destination) throws JMSException {

	checkLegalOperation();

		MantaMessageProducer mmp = null;
		if (destination == null ) //need producer with no specific service
			mmp = new MantaMessageProducer(manta.getMessageId(),this);

		else if (!destination.toString().startsWith(MantaConnection.TMP_DESTINATION_PREFIX)) {
			MantaService service = null;
			if(destination instanceof Queue){
				owningConnection.authorize(SecurityActionTypes.ACTION_CREATE_PRODUCER_FOR_QUEUE,destination.toString() );
				service  = manta.getService(destination.toString(),MantaService.SERVICE_TYPE_QUEUE);
			}else{
				owningConnection.authorize(SecurityActionTypes.ACTION_CREATE_PRODUCER_FOR_TOPIC,destination.toString() );
				service  = manta.getService(destination.toString(),MantaService.SERVICE_TYPE_TOPIC);
			// can't create producer to a hierarchy topic with wildcards
			}if (service == null){
				throw new JMSException("MNJMS00079 : FAILED ON METHOD createProducer() FOR DESTINATION "+destination);
			}
			ServiceProducer sActor = ServiceProducer.createNew(service);

			try {
				if (log.isInfoEnabled()) {
					log.info("Created local producer "+sActor);
				}
				manta.advertiseService(sActor);
				mmp = new MantaMessageProducer(sActor.getId(),this,destination,sActor);
			}
			catch (MantaException me) {
				mmp = null;
				sActor = null;
				throw new JMSException("MNJMS0007A : FAILED ON METHOD createProducer(). ERROR TEXT : "+me.getMessage());
			}
		}

		else { //this is a temp destination!
			ServiceProducer producer =null ;
			if(destination instanceof Queue){
				owningConnection.authorize(SecurityActionTypes.ACTION_CREATE_PRODUCER_FOR_QUEUE);
				producer = new ServiceProducer( manta.getAgentName(),destination.toString(), MantaService.SERVICE_TYPE_QUEUE);
			}else{
				owningConnection.authorize(SecurityActionTypes.ACTION_CREATE_PRODUCER_FOR_TOPIC);
				producer = new ServiceProducer( manta.getAgentName(),destination.toString(), MantaService.SERVICE_TYPE_TOPIC);
			}
			mmp = new MantaMessageProducer(
					manta.getMessageId(),
					this, destination,
					producer);

		}
		messageProducers.put(mmp.getClientId(),mmp);

		// this delay is added to prevent a situation that in
		// NAD producer will start sending messages before consumers
		// are ready to receive thyem. (Bug 543)
		int delay = MantaAgent.getInstance().getSingletonRepository().getConfigManager().getIntProperty("jms.producer_discovery_delay",100);
		try {
			Thread.sleep(delay);
		} catch (InterruptedException e) {
			
			//PATCH: Don't swallow interrupts
			Thread.currentThread().interrupt();
			
			if (log.isWarnEnabled()) {
				log.warn("Interrupted during sleep.");
			}
		}
		return mmp;
	}//createProducer

	/**
	 * Creates a <CODE>MessageConsumer</CODE> for the specified destination.
	 * Since <CODE>Queue</CODE> and <CODE>MantaTopic</CODE> both inherit
	 * from <CODE>Destination</CODE>, they can be used in the destination
	 * parameter to create a <CODE>MessageConsumer</CODE>.
	 *
	 * @param destination
	 *            the <CODE>Destination</CODE> to access.
	 *
	 * @exception JMSException
	 *                if the session fails to create a consumer due to some
	 *                internal error.
	 * @exception InvalidDestinationException
	 *                if an invalid destination is specified.
	 *
	 * @since 1.1
	 */
	public MessageConsumer createConsumer(Destination destination) throws JMSException {

		return createConsumer(destination,null,false);

	}//createConsumer

	/**
	 * Creates a <CODE>MessageConsumer</CODE> for the specified destination,
	 * using a message selector. Since <CODE>Queue</CODE> and <CODE>
	 * MantaTopic</CODE> both inherit from <CODE>Destination</CODE>, they
	 * can be used in the destination parameter to create a <CODE>
	 * MessageConsumer</CODE>.
	 *
	 * <P>
	 * A client uses a <CODE>MessageConsumer</CODE> object to receive
	 * messages that have been sent to a destination.
	 *
	 * @param destination
	 *            the <CODE>Destination</CODE> to access
	 * @param messageSelector
	 *            only messages with properties matching the message selector
	 *            expression are delivered. A value of null or an empty string
	 *            indicates that there is no message selector for the message
	 *            consumer.
	 *
	 *
	 * @exception JMSException
	 *                if the session fails to create a MessageConsumer due to
	 *                some internal error.
	 * @exception InvalidDestinationException
	 *                if an invalid destination is specified.
	 *
	 * @exception InvalidSelectorException
	 *                if the message selector is invalid.
	 *
	 * @since 1.1
	 */
	public final MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {

		return createConsumer(destination,messageSelector,false);

	}//createConsumer


	/**
	 * Creates <CODE>MessageConsumer</CODE> for the specified destination,
	 * using a message selector. This method can specify whether messages
	 * published by its own connection should be delivered to it, if the
	 * destination is a topic.
	 * <P>
	 * Since <CODE>Queue</CODE> and <CODE>MantaTopic</CODE> both inherit
	 * from <CODE>Destination</CODE>, they can be used in the destination
	 * parameter to create a <CODE>MessageConsumer</CODE>.
	 * <P>
	 * A client uses a <CODE>MessageConsumer</CODE> object to receive
	 * messages that have been published to a destination.
	 *
	 * <P>
	 * In some cases, a connection may both publish and subscribe to a topic.
	 * The consumer <CODE>NoLocal</CODE> attribute allows a consumer to
	 * inhibit the delivery of messages published by its own connection. The
	 * default value for this attribute is False. The <CODE>noLocal</CODE>
	 * value must be supported by destinations that are topics.
	 *
	 * @param destination
	 *            the <CODE>Destination</CODE> to access
	 * @param messageSelector
	 *            only messages with properties matching the message selector
	 *            expression are delivered. A value of null or an empty string
	 *            indicates that there is no message selector for the message
	 *            consumer.
	 * @param NoLocal -
	 *            if true, and the destination is a topic, inhibits the
	 *            delivery of messages published by its own connection. The
	 *            behavior for <CODE>NoLocal</CODE> is not specified if the
	 *            destination is a queue.
	 *
	 * @exception JMSException
	 *                if the session fails to create a MessageConsumer due to
	 *                some internal error.
	 * @exception InvalidDestinationException
	 *                if an invalid destination is specified.
	 *
	 * @exception InvalidSelectorException
	 *                if the message selector is invalid.
	 *
	 * @since 1.1
	 *
	 */
	public final MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {

		checkLegalOperation();
		if (destination==null)
			throw new InvalidDestinationException("MNJMS0007B : FAILED ON METHOD createConsumer(). NULL DESTINATION WAS SUPPLIED.");

		//this will throw an InvalidSelectorException if the selector is bad.
		if(messageSelector!= null)
			messageSelector = messageSelector.trim();
		Selector s = new Selector(messageSelector);
		MantaMessageConsumer mmc;
		//Check that the destination exists on the network.
		MantaService service = null;
		byte type;
		if(destination instanceof Queue){
			owningConnection.authorize(SecurityActionTypes.ACTION_CREATE_CONSUMER_FOR_QUEUE,destination.toString() );
			service = manta.getService(destination.toString(), MantaService.SERVICE_TYPE_QUEUE);
			type = MantaService.SERVICE_TYPE_QUEUE;
		}else{
			owningConnection.authorize(SecurityActionTypes.ACTION_CREATE_CONSUMER_FOR_TOPIC,destination.toString() );
			service = manta.getService(destination.toString(), MantaService.SERVICE_TYPE_TOPIC);
			type = MantaService.SERVICE_TYPE_TOPIC;
		}

		// For topics we get a service object only if the topic is real,
		// meaning the name doesn't contain wildcards.
		boolean isWildCard = (type == MantaService.SERVICE_TYPE_TOPIC) &&
		                     VirtualTopicManager.isWildCardTopic(destination.toString());
		if (service == null && !isWildCard)
			throw new JMSException("MNJMS0007C : FAILED ON METHOD createConsumer(). COULD NOT REGISTER ON DESTINATION "+destination);

		ServiceConsumer sActor;
		try {
			sActor = new ServiceConsumer(manta.getAgentName(), manta.getDomainName(),
					destination.toString() , type,(byte)getAcknowledgeMode());
			if (type==MantaService.SERVICE_TYPE_TOPIC) {
				//register the session as a listener on that service

				registerListener(sActor.getId());


			}
			sActor.setSelectorStatment(messageSelector);

			mmc = new MantaMessageConsumer(sActor.getId(),this, destination,messageSelector,noLocal,sActor);
			messageConsumers.put(mmc.getClientId(),mmc);
			if (log.isInfoEnabled()) {
				log.info("Created local consumer "+sActor);
			}
			manta.advertiseService(sActor);
		}
		catch (MantaException me) {
			mmc = null;
			sActor = null;
			throw new JMSException("MNJMS0007D : FAILED ON METHOD createConsumer(). ERROR TEXT : "+me.getMessage());

		}
		return mmc;
	}//createConsumer

	/**
	 * Creates a queue identity given a <CODE>Queue</CODE> name.
	 *
	 * <P>
	 * This facility is provided for the rare cases where clients need to
	 * dynamically manipulate queue identity. It allows the creation of a queue
	 * identity with a provider-specific name. Clients that depend on this
	 * ability are not portable.
	 *
	 * <P>
	 * Note that this method is not for creating the physical queue. The
	 * physical creation of queues is an administrative task and is not to be
	 * initiated by the JMS API. The one exception is the creation of temporary
	 * queues, which is accomplished with the <CODE>createTemporaryQueue
	 * </CODE> method.
	 *
	 * @param queueName
	 *            the name of this <CODE>Queue</CODE>
	 *
	 * @return a <CODE>Queue</CODE> with the given name
	 *
	 * @exception JMSException
	 *                if the session fails to create a queue due to some
	 *                internal error.
	 * @since 1.1
	 */
	public Queue createQueue(String queueName) throws JMSException {

		checkLegalOperation();
		return new MantaQueue(queueName);
	}//createQueue

	/**
	 * Creates a topic identity given a <CODE>MantaTopic</CODE> name.
	 *
	 * <P>
	 * This facility is provided for the rare cases where clients need to
	 * dynamically manipulate topic identity. This allows the creation of a
	 * topic identity with a provider-specific name. Clients that depend on
	 * this ability are not portable.
	 *
	 * <P>
	 * Note that this method is not for creating the physical topic. The
	 * physical creation of topics is an administrative task and is not to be
	 * initiated by the JMS API. The one exception is the creation of temporary
	 * topics, which is accomplished with the <CODE>createTemporaryTopic
	 * </CODE> method.
	 *
	 * @param topicName
	 *            the name of this <CODE>MantaTopic</CODE>
	 *
	 * @return a <CODE>MantaTopic</CODE> with the given name
	 *
	 * @exception JMSException
	 *                if the session fails to create a topic due to some
	 *                internal error.
	 * @since 1.1
	 */
	public Topic createTopic(String topicName) throws JMSException {

		checkLegalOperation();
		return new MantaTopic(topicName);
	}//createTopic


	/**
	 * Creates a durable subscriber to the specified topic.
	 *
	 * <P>
	 * If a client needs to receive all the messages published on a topic,
	 * including the ones published while the subscriber is inactive, it uses a
	 * durable <CODE>TopicSubscriber</CODE>. The JMS provider retains a
	 * record of this durable subscription and insures that all messages from
	 * the topic's publishers are retained until they are acknowledged by this
	 * durable subscriber or they have expired.
	 *
	 * <P>
	 * Sessions with durable subscribers must always provide the same client
	 * identifier. In addition, each client must specify a name that uniquely
	 * identifies (within client identifier) each durable subscription it
	 * creates. Only one session at a time can have a <CODE>TopicSubscriber
	 * </CODE> for a particular durable subscription.
	 *
	 * <P>
	 * A client can change an existing durable subscription by creating a
	 * durable <CODE>TopicSubscriber</CODE> with the same name and a new
	 * topic and/or message selector. Changing a durable subscriber is
	 * equivalent to unsubscribing (deleting) the old one and creating a new
	 * one.
	 *
	 * <P>
	 * In some cases, a connection may both publish and subscribe to a topic.
	 * The subscriber <CODE>NoLocal</CODE> attribute allows a subscriber to
	 * inhibit the delivery of messages published by its own connection. The
	 * default value for this attribute is false.
	 *
	 * @param topic
	 *            the non-temporary <CODE>MantaTopic</CODE> to subscribe to
	 * @param name
	 *            the name used to identify this subscription
	 *
	 * @exception JMSException
	 *                if the session fails to create a subscriber due to some
	 *                internal error.
	 * @exception InvalidDestinationException
	 *                if an invalid topic is specified.
	 *
	 * @since 1.1
	 */
	public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {

		return createDurableSubscriber(topic,name,null,false);
	}//createDurableSubscriber


	/**
	 * Creates a durable subscriber to the specified topic, using a message
	 * selector and specifying whether messages published by its own connection
	 * should be delivered to it.
	 *
	 * <P>
	 * If a client needs to receive all the messages published on a topic,
	 * including the ones published while the subscriber is inactive, it uses a
	 * durable <CODE>TopicSubscriber</CODE>. The JMS provider retains a
	 * record of this durable subscription and insures that all messages from
	 * the topic's publishers are retained until they are acknowledged by this
	 * durable subscriber or they have expired.
	 *
	 * <P>
	 * Sessions with durable subscribers must always provide the same client
	 * identifier. In addition, each client must specify a name which uniquely
	 * identifies (within client identifier) each durable subscription it
	 * creates. Only one session at a time can have a <CODE>TopicSubscriber
	 * </CODE> for a particular durable subscription. An inactive durable
	 * subscriber is one that exists but does not currently have a message
	 * consumer associated with it.
	 *
	 * <P>
	 * A client can change an existing durable subscription by creating a
	 * durable <CODE>TopicSubscriber</CODE> with the same name and a new
	 * topic and/or message selector. Changing a durable subscriber is
	 * equivalent to unsubscribing (deleting) the old one and creating a new
	 * one.
	 *
	 * @param topic
	 *            the non-temporary <CODE>MantaTopic</CODE> to subscribe to
	 * @param name
	 *            the name used to identify this subscription
	 * @param messageSelector
	 *            only messages with properties matching the message selector
	 *            expression are delivered. A value of null or an empty string
	 *            indicates that there is no message selector for the message
	 *            consumer.
	 * @param noLocal
	 *            if set, inhibits the delivery of messages published by its
	 *            own connection
	 *
	 * @exception JMSException
	 *                if the session fails to create a subscriber due to some
	 *                internal error.
	 * @exception InvalidDestinationException
	 *                if an invalid topic is specified.
	 * @exception InvalidSelectorException
	 *                if the message selector is invalid.
	 *
	 * @since 1.1
	 */
	public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {

		checkLegalOperation();
		owningConnection.authorize(SecurityActionTypes.ACTION_SUBSCRIBE_DURABLE_ON_TOPIC,topic.toString() );

		if (topic==null)
			throw new InvalidDestinationException("MNJMS0007E : FAILED ON METHOD createDurableSubscriber(). A NULL TOPIC WAS SPECIFIED.");

		//this will throw an InvalidSelectorException when the selector is bad
        //Aviad - add the same fix for empty selector String as for createConsumer
        if(messageSelector!= null) {
			messageSelector = messageSelector.trim();
        }
        Selector s = new Selector(messageSelector);

		// removing an old subscription before creating a new one
		cleanSubscriptionIfNeeded(topic, name, messageSelector, noLocal);

		// now we can create a new subscription.
		MantaTopicSubscriber newSub = null;
		MantaService service = manta.getService(topic.toString(), MantaService.SERVICE_TYPE_TOPIC);
		// For topics we get a service object only if the topic is real,
		// meaning the name doesn't contain wildcards.
		boolean isWildCard = VirtualTopicManager.isWildCardTopic(topic.toString());
		if (service == null && !isWildCard)
			throw new InvalidDestinationException("MNJMS0007F : FAILED ON METHOD createDurableSubscriber(). TOPIC "+topic+" NOT VALID.");

		//register as a durable subscriber for that topic.
		ServiceConsumer sActor;

		try {
			sActor = new ServiceConsumer(manta.getAgentName(), manta.getDomainName(),
					topic.toString() , MantaService.SERVICE_TYPE_TOPIC,(byte)getAcknowledgeMode(),name);
			DurableSubscribers.put(name,sActor);
			registerListener(sActor.getId());
			newSub = new MantaTopicSubscriber(sActor.getId(),this,topic,noLocal,true,name,messageSelector,sActor);
			//register ServiceActor and subscriber in the internal registries.
			messageConsumers.put(newSub.getClientId(),newSub);
			sActor.setSelectorStatment(messageSelector);
			//bug:421 sActor.setNoLocal(noLocal);
			if (log.isInfoEnabled()) {
				log.info("Created local durable subscriber "+sActor);
			}
			manta.advertiseService(sActor);

		}
		catch (MantaException me) {
			newSub = null;
			sActor = null;
			throw new JMSException("MNJMS00080 : FAILED ON METHOD CreateDurableSubscriber(). ERROR TEXT : "+me.getMessage());
		}
		return newSub;

	}//createDurableSubscriber


	private void cleanSubscriptionIfNeeded(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
		MantaTopicSubscriber old = (MantaTopicSubscriber)messageConsumers.get(name);
		if (old != null) {
			// if the subscription's topic/selector/noLocal were changed
			// we need to recreate the subscription. If the application tries
			// to create a new durable subscriber but with the same name
			// an exception is thrown.
			if (//bug:421 old.getNoLocal() != noLocal ||
				!checkEqual(old.getMessageSelector(), messageSelector) ||
				!((Topic)old.getDestination()).getTopicName().equals(topic.getTopicName())) {
				if (log.isDebugEnabled()) {
					log.debug("The durable subscriber '"+name+"' was changed. Deleting old subscription and creating new subscription.");
				}
				old.close();
				unsubscribe(name);
				return;
			}
			else {
				throw new JMSException("A durable subscriber with the name '"+name+"' already exists.");
			}
		}
		// if we are here no message consumers was up.
		// see that a subscription exists. if it's parameters are chaged delete it.
		ServiceConsumer durable = (ServiceConsumer)DurableSubscribers.get(name);
		if(durable != null) {
			if (//bug:421 durable.getNoLocal() != noLocal ||
				!checkEqual(durable.getSelectorStatment(), messageSelector) ||
				!durable.getServiceName().equals(topic.getTopicName())) {
				if (log.isDebugEnabled()) {
					log.debug("The durable subscriber '"+name+"' was changed. Deleting old subscription and creating new subscription.");
				}
				unsubscribe(name);
			}
		}
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
	 *
	 * this method is the receiving method for getting messages.
	 * 	all consumers (that are not listeners) use this method in order
	 * 	to receive their messages.
	 * 	corresponding to the spec, this is synchronized, as only one
	 * 	consumer is allowed access to the session at any given time
	 * 	listeners should not be defined on a session that has synch (blocking)
	 * 	receivers - so that's alright.
	 *
	 * @param destination - the queue/topic to listen on
	 * @param timeout - -1  : indefinite.
	 * 				  - 0   : nowait receive.
	 * 				  - positive millis to wait.
	 * @return a Message.
	 */
	public MantaBusMessage receive(ServiceConsumer consumer, long timeout) throws JMSException, MantaException
	{
		checkLegalOperation();
		MantaBusMessage msg = null;
		synchronized (lockMonitor) {
			if (isClosing || isClosed)
				return null;

			if (!owningConnection.isStarted()) {
				long startTime = System.currentTimeMillis();
				try {
					lockMonitor.wait(timeout);
				} catch (InterruptedException e) {
					
					//PATCH: Don't swallow interrupts
					Thread.currentThread().interrupt();
					
					if (log.isErrorEnabled()) {
						log.error("Error while waiting for the session to resume. ", e);
					}
				}
				timeout = timeout-(System.currentTimeMillis()-startTime);
				if (timeout < 1000) //not enough for a receiveNoWait even
					return null;
			}

			//check first if this is a topic. if it is - there's a special
			//handling for a topic's receive.
			if ( (manta.getService(consumer.getServiceName(), consumer.getType())).getServiceType()==MantaService.SERVICE_TYPE_TOPIC) {

				if (timeout==-1) {
					return null; //we can not do that in our imp.
				}


				//deregister the topic subscriber as a listener on the topic.
				//this is done because, when going into a receive on a topic, you
				//can not use it to get asynch, but - if we leave it in - then the consumer
				//itself will get the message and not our internal listener.
				removeSessionFrom(consumer.getId());
				//register a nice listener.
				ReceiveListener listen = new ReceiveListener();
				manta.subscribeMessageListener(listen,consumer.getId());
				msg = listen.waitForInfo(timeout);
			}
			else { //this is a queue - a regular receive is good enough.
				if (timeout == 0) {
 				    msg = manta.receive(consumer);
				}
				else if (timeout == -1) {
					msg = manta.receiveNoWait(consumer);
				}
				else { //timeout was given
					msg = manta.receive(consumer,timeout);
				}
			}
		}

		if (msg != null) {
			// start a transaction if needed
			this.startLocalTransactionIfNeeded();

			if (sessionAcknowledgementMode == Session.CLIENT_ACKNOWLEDGE || this.getTransacted()) {
				synchronized (unackedMessages) {
					unackedMessages.add(msg);
				}
			}
		}

		return msg;
	}

	/**
	 * Creates a <CODE>QueueBrowser</CODE> object to peek at the messages on
	 * the specified queue.
	 *
	 * @param queue
	 *            the <CODE>queue</CODE> to access
	 *
	 *
	 * @exception JMSException
	 *                if the session fails to create a browser due to some
	 *                internal error.
	 * @exception InvalidDestinationException
	 *                if an invalid destination is specified
	 *
	 * @since 1.1
	 */
	public QueueBrowser createBrowser(Queue queue) throws JMSException
	{
		return createBrowser(queue, null);
	}

	/**
	 * Creates a <CODE>QueueBrowser</CODE> object to peek at the messages on
	 * the specified queue using a message selector.
	 *
	 * @param queue
	 *            the <CODE>queue</CODE> to access
	 *
	 * @param messageSelector
	 *            only messages with properties matching the message selector
	 *            expression are delivered. A value of null or an empty string
	 *            indicates that there is no message selector for the message
	 *            consumer.
	 *
	 * @exception JMSException
	 *                if the session fails to create a browser due to some
	 *                internal error.
	 * @exception InvalidDestinationException
	 *                if an invalid destination is specified
	 * @exception InvalidSelectorException
	 *                if the message selector is invalid.
	 *
	 * @since 1.1
	 */
	public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException
	{
		checkLegalOperation();
		owningConnection.authorize(SecurityActionTypes.ACTION_CREATE_BROSWER_FOR_QUEUE,queue.toString() );

		if (queue == null)
			throw new InvalidDestinationException("MNJMS00081 : FAILED ON METHOD createBrowser(). A NULL QUEUE WAS SPECIFIED.");

		MantaQueueBrowser mqb = null;
		MantaService service = manta.getService(queue.toString(), MantaService.SERVICE_TYPE_QUEUE);
		if (service == null)
			throw new InvalidDestinationException("MNJMS00082 : FAILED ON METHOD createBrowser(). QUEUE "+queue+" NOT VALID.");

		ServiceConsumer sActor;

		try {
			sActor = new ServiceConsumer(manta.getAgentName(), manta.getDomainName(),
					service.getServiceName() , service.getServiceType(),(byte)getAcknowledgeMode());
			mqb = new MantaQueueBrowser(sActor.getId(),this, queue,messageSelector,sActor);
			sActor.setSelectorStatment(messageSelector);
			if (log.isInfoEnabled()) {
				log.info("Created local queue browser "+sActor);
			}
			manta.advertiseService(sActor);

		}
		catch (MantaException me) {
			mqb = null;
			sActor = null;
			throw new JMSException("MNJMS00083 : FAILED ON METHOD createBrowser(). ERROR TEXT : "+me.getMessage());

		}



		return mqb;

	}


	/**
	 * Creates a <CODE>TemporaryQueue</CODE> object. Its lifetime will be
	 * that of the <CODE>MantaConnection</CODE> unless it is deleted
	 * earlier.
	 *
	 * @return a temporary queue identity
	 *
	 * @exception JMSException
	 *                if the session fails to create a temporary queue due to
	 *                some internal error.
	 *
	 * @since 1.1
	 */
	public TemporaryQueue createTemporaryQueue() throws JMSException {

		checkLegalOperation();

		return owningConnection.addTempQueue();

	}//createTemporaryQueue


	/**
	 * Creates a <CODE>TemporaryTopic</CODE> object. Its lifetime will be
	 * that of the <CODE>MantaConnection</CODE> unless it is deleted
	 * earlier.
	 *
	 * @return a temporary topic identity
	 *
	 * @exception JMSException
	 *                if the session fails to create a temporary topic due to
	 *                some internal error.
	 *
	 * @since 1.1
	 */
	public TemporaryTopic createTemporaryTopic() throws JMSException {

		checkLegalOperation();
		TemporaryTopic t = owningConnection.addTempTopic();
		return new MantaTemporaryTopic(t.getTopicName(),this.owningConnection);
	}//createTemporaryTopic


	/**
	 * Unsubscribes a durable subscription that has been created by a client.
	 *
	 * <P>
	 * This method deletes the state being maintained on behalf of the
	 * subscriber by its provider.
	 *
	 * <P>
	 * It is erroneous for a client to delete a durable subscription while
	 * there is an active <CODE>MessageConsumer</CODE> or <CODE>
	 * TopicSubscriber</CODE> for the subscription, or while a consumed
	 * message is part of a pending transaction or has not been acknowledged in
	 * the session.
	 *
	 * @param name
	 *            the name used to identify this subscription
	 *
	 * @exception JMSException
	 *                if the session fails to unsubscribe to the durable
	 *                subscription due to some internal error.
	 * @exception InvalidDestinationException
	 *                if an invalid subscription name is specified.
	 *
	 * @since 1.1
	 */
	public  void unsubscribe(String name) throws JMSException {

		checkLegalOperation();
	   	// implemented by Amir Shevat
		ServiceActor durable =  (ServiceActor) DurableSubscribers.remove(name);
		if(durable != null){
			if (log.isInfoEnabled())
				log.info("Unsubscribing durable service consumer: "+durable);
			try {
				manta.recallDurableSubscription(durable);
			} catch (MantaException me) {
				throw new JMSException("MNJMS00083 : FAILED ON METHOD unsubscribe(). FROM TOPIC "+name+" ERROR TEXT : "+me.getMessage());

			}
		}

	}//unsubscribe

	final void removeSessionFrom(String dest) {
	   manta.unsubscribeFromTopic(this,dest);
	}

	/*
	 * The send()method is used by all MessageProviders to send messages to their various
	 * destinations via the session.
	 * The spec enforces a messaging order. Therefore, the send method must be
	 * synchronized.
	 *
	 * @param msg - the message to send.
	 */
	 synchronized void sendMessage(ServiceProducer sp, Message orig) throws JMSException {

		checkLegalOperation();

		MantaMessage msg;
		if(orig instanceof MantaMessage){
			msg = ((MantaMessage)orig).makeCopy();
		}else{
			msg = fromForeignMsgToManta(orig);
		}

		Destination dest = msg.getJMSDestination();

		//if transacted - cache message without sending until commit.

		this.startLocalTransactionIfNeeded();

		if (this.sessionAcknowledgementMode == SESSION_TRANSACTED) {
			msg.setJMSMessageID("ID:in-transaction");
			orig.setJMSMessageID("ID:in-transaction");
			heldMessages.add(new HeldMessage(sp,msg));
			if (log.isDebugEnabled()) {
				log.debug("Transacted session: Caching message until commit is invoked. Message="+msg);
			}
		}
		else{
			//send the message to the owning connection for sending:
			//differentiate if topic or queue.
            //Aviad - the time stamp should be set duringthe send for all session types - moving to MantaMessageProducer.send
            //msg.setJMSTimestamp(SystemTime.gmtCurrentTimeMillis());
			MantaBusMessage mbm = prepareMessageForSending(msg);
			orig.setJMSMessageID(msg.JMSMessageId);
			msg.setWriteableState(false);
			if (sp.getServiceType()==MantaService.SERVICE_TYPE_QUEUE) {
				try {
					if (log.isDebugEnabled()) {
						log.debug("About to send massage to queue. Message ID="+mbm.getMessageId()+", Queue="+sp.getServiceName());
					}
					manta.enqueueMessage(mbm,
							             sp,
							             (byte)msg.getJMSDeliveryMode(),
							             (byte)msg.getJMSPriority(),
							             msg.getJMSExpiration());
				}
				catch (MantaException me) {
					mbm = null;
					throw new JMSException("MNJMS00084 : FAILED ON METHOD sendMessage(). ERROR TEXT : "+me.getMessage());
				}
			}
			//this is a topic - use the publish method.
			else {
				try {
					if (log.isDebugEnabled()) {
						log.debug("Sending massage to topic. Message ID="+mbm.getMessageId()+", Topic="+sp.getServiceName());
					}
					manta.publish(mbm,sp);
				}
				catch (MantaException me) {
					mbm = null;
					throw new JMSException("MNJMS00085 : COULD NOT PUBLISH TO "+sp.getServiceName()+" FAILED ON METHOD sendMessage(). ERROR TEXT : "+me.getMessage());
				}
			}
			//after the client has sent the message, and upon its
			//return, the message should be writable again.
		}
		if (orig instanceof MantaMessage)
			((MantaMessage) orig).setWriteableState(true);
	 }


	 void ackAllMessages(Collection msgs) throws JMSException {
		 Iterator ackIterator = msgs.iterator();
		 while (ackIterator.hasNext()) {
			 MantaBusMessage mbm = (MantaBusMessage)ackIterator.next();
			 owningConnection.ack(mbm);
		 }
		 msgs.clear();
	 }

	 //used to send all messages in case of a transacted session.
	 //this goes over the repository and sends all messages
	 //withheld - waiting for commit.

	 protected void sendAllMessages(Collection msgs) throws JMSException {

	 	Destination dest;
	 	MantaMessage msg;
	 	Iterator msgIterator = msgs.iterator();
	 	HeldMessage hm;

	 	while (msgIterator.hasNext()) {
			hm = (HeldMessage)msgIterator.next();
			msg = hm.msg;
	 		dest = msg.getJMSDestination();
//Aviad this should be done only during send  and not during commit             
//	 		msg.setJMSTimestamp(System.currentTimeMillis());
	 		MantaBusMessage mbm = prepareMessageForSending(msg);
	 		ServiceProducer sp = hm.service;
	 		if (dest instanceof Queue) {
	 			try {
	 				manta.enqueueMessage(mbm,
										 sp,
										 (byte)msg.getJMSDeliveryMode(),
										 (byte)msg.getJMSPriority(),
										 msg.getJMSExpiration());
	 			}
	 			catch (MantaException me) {
	 				mbm = null;
	 				sp = null;
					throw new JMSException("MNJMS00086 : FAILED ON METHOD sendAllMessages(). ERROR TEXT : "+me.getMessage());
	 			}
	 		}
	 		//this is a topic - use the publish method.
	 		else {
	 			try {
	 				manta.publish(mbm,sp);
	 			}
	 			catch (Exception me) { //need to be MantaException
	 				mbm = null;
					throw new JMSException("MNJMS00087 : FAILED ON METHOD sendAllMessages(). ERROR TEXT : "+me.getMessage());
	 			}
	 		}
	 		//delete the message that was just sent
	 		msgIterator.remove();
	 	}
	 }


	/*
	 * This method is called by every other method on the session, and only allows
	 * operations when the session is functional.
	 * If the session is closed, or in the process of closing - this method
	 * will indicate so with an exception.
	 *
	 * @throws JMSException - when the session is in an invalid state.
	 */
	 void checkLegalOperation() throws JMSException
	{
		if (isClosed || isClosing)
		  throw new IllegalStateException("MNJMS00088 : OPERATION FAILED ON METHOD checkLegalOperation(). SESSION IS CLOSED.");
	}

	 /**
	 * When registering a session as a listener - you have to have this
	 * method - so the listened objects can call on it.
	 * After doing so, the session should pass the message on to the correct
	 * consumer, as identified by the message credentials.
	 */
	public void onMessage(MantaBusMessage msg) {
		innerQueue.enqueue(msg);
	}


	void ackOrHold(MantaBusMessage mbm) throws JMSException {
		if (getTransacted() || sessionAcknowledgementMode == Session.CLIENT_ACKNOWLEDGE) {
			if (mbm != null) {
				synchronized(unackedMessages) {
					unackedMessages.add(mbm);
				}
			}
		}
		else {
			ackMessage(mbm);
		}
	}
	/**
	 * This method is used to ack messages that the client wishes to
	 * ack by himself - the ACKNOWLEDGE messages.
	 * All it actually does is ask the MantaAgent itself for the ack
	 * method, after checking that the  mode is on.
	 *
	 * @param msg
	 * @throws JMSException
	 */
	 void ackMessage (MantaBusMessage msg) throws JMSException {
		checkLegalOperation();

		//if (sessionTransactedMode)
		if (getTransacted())
			return;

		if (sessionAcknowledgementMode != Session.CLIENT_ACKNOWLEDGE) {
			if (msg != null)
				owningConnection.ack(msg);
		}
		else {
			synchronized (unackedMessages) {
				Iterator ackIterator = unackedMessages.iterator();
				while (ackIterator.hasNext()) {
					MantaBusMessage cbm = (MantaBusMessage)ackIterator.next();
					owningConnection.ack(cbm);
				}
				unackedMessages.clear();
			}
		}
	}


	//topic specific methods:

	/**
	 * Creates a TopicPublisher for this session
	 */

	public TopicPublisher createPublisher(Topic topic) throws JMSException{
		return (TopicPublisher) createProducer(topic);
	}//createPublisher

	/**
	 * Creates a TopicSubscriber for this session
	 */
 	public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
    	return (TopicSubscriber) createConsumer(topic);
    }

 	/**
	 * Creates a TopicSubscriber for this session
	 */
   	public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {

   		//create the consumer as subscriber
   		TopicSubscriber theSub = (TopicSubscriber)createConsumer(topic, messageSelector, noLocal);

   		return theSub;
   	}


	//queue specific stuff

   	/**
   	 * Creates a QueueReceiver on this session.
   	 */
	public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
      return (QueueReceiver) createConsumer(queue, messageSelector);
	}

	/**
   	 * Creates a QueueReceiver on this session.
   	 */
	public QueueReceiver createReceiver(Queue queue) throws JMSException
	{
      return (QueueReceiver) createConsumer(queue);
	}

	/**
   	 * Creates a QueueSender on this session.
   	 */
	public QueueSender createSender(Queue queue) throws JMSException
	{
      return (QueueSender) createProducer(queue);
	}


	/*
	 * This method will prepare a MantaBusMessage for sending.
	 *
	 * @param message the JMS message to be sent
	 * @return the new MantaBusMessage
	 */
	private MantaBusMessage prepareMessageForSending(Message message) throws JMSException {


		MantaBusMessage mantaBusMessage = manta.getMantaBusMessage();
		message.setJMSMessageID("ID:"+mantaBusMessage.getMessageId());
		mantaBusMessage.setPayload((Byteable)message);
		mantaBusMessage.setMessageType(MantaBusMessageConsts.MESSAGE_TYPE_CLIENT);
		mantaBusMessage.setPriority((byte)(message.getJMSPriority()));
		mantaBusMessage.addHeader(MantaBusMessageConsts.HEADER_NAME_PAYLOAD_TYPE,MantaBusMessageConsts.PAYLOAD_TYPE_JMS);
		mantaBusMessage.setDeliveryMode((byte) message.getJMSDeliveryMode());
		mantaBusMessage.setValidUntil(message.getJMSExpiration());

		return mantaBusMessage;

	}//prepareMessageForSend

	/*
	 * a consumer uses this method to remove itself from the session upon closing.
	 */
	 void removeConsumer(MantaMessageConsumer mc) throws JMSException {

		 //if (isClosed || isClosing)
		 if (isClosed) {
			 return;
		 }

		 messageConsumers.remove(mc.getClientId());
		 manta.unsubscribeMessageListener(this, mc.getService().getId());

		 if (!mc.getService().getServiceName().startsWith(MantaConnection.TMP_DESTINATION_PREFIX)) {
			 // allow acks get there before the recall does
			 try {
				 Thread.sleep(500);
			 } catch (InterruptedException ie) {
				 
					//PATCH: Don't swallow interrupts
					Thread.currentThread().interrupt();
					
				 if(log.isInfoEnabled()) {
					 log.info("removeConsumer() : acks may have been lost for this consumer - "+mc);
				 }
			 }
		 }

		 // remove from manta agent
		 try {
			 if(mc.getService()!= null){
				 if (log.isInfoEnabled()) {
					 if (!mc.getService().isDurable()) {
						 log.info("Recalling local consumer "+mc.getService());
					 }
					 else {
						 log.info("Recalling local durable subscriber "+mc.getService());
					 }
				 }
				 manta.recallService(mc.getService());
			 }
		 }
		 catch (MantaException ce) {
		 	 if (log.isErrorEnabled()) {
			  	 log.error("removeConsumer(): could not remove service "+mc.getService().getServiceName(), ce);
		 	 }
			 throw new JMSException("MNJMS00077 : FAILED ON close(). CONSUMER ON SERVICE "+
						mc.getService().getServiceName()+" WAS NOT RECALLED. ERROR TEXT : "+ce.getMessage());
		 }
	}

	/*
	 * this is used specifically for a queue browser, since all consumers
	 * go to the same repository, but a queue browser is not a consumer.
	 */
	 void removeBrowser(MantaQueueBrowser qb) throws JMSException {

		 if (log.isInfoEnabled()) {
			log.info("Recalling local queue browser "+qb.service);
		 }
		try {
	 		manta.recallService(qb.service);
	 	}
	 	catch (MantaException me) {
			if (log.isErrorEnabled()) {
				log.error("removeBrowser(): could not remove browser "+qb.getService(),me);
			}
	 		throw new JMSException(me.getMessage());
	 	}
	}

	 /*
	  * used to remove a producer from the session.
	  *
	  * @param mp - the producer to remove
	  */
	 void removeProducer(MantaMessageProducer mp) throws JMSException {

		 //if (isClosed||isClosing)
		 if (isClosed)
	 		return;

		messageProducers.remove(mp.getClientId());

	    //remove from the connection.
		try {
			if(mp.getService()!= null){
				if (log.isInfoEnabled()) {
					log.info("Recalling local producer "+mp.getService());
				}
				manta.recallService(mp.getService());
			}
		}
		catch (MantaException ce) {
			if (log.isErrorEnabled()) {
				log.error("removeProducer(): could not remove service "+mp.getService().getServiceName(), ce);
			}
			throw new JMSException("MNJMS00076 : FAILED ON close(). PRODUCER ON SERVICE "+
					mp.getService().getServiceName()+" WAS NOT RECALLED. ERROR TEXT : "+ce.getMessage());
		}

	}

	 /*
	  * Get messages for a queue browser.
	  * @param qb - the QueueBrowser to get the messages for
	  * @return - an Enumeration containing the messages
	  *
	  * @throws JMSException
	  */
	 Enumeration getMessagesFor(MantaQueueBrowser qb) throws JMSException {
	 	try {

	 		return manta.peekAtQueue(qb.getService());
	 	}
	 	catch (MantaException me) {
	 	   throw new JMSException("MNJMS0008A : FAILED ON METHOD getMessagesFor(). ERROR TEXT : "+me.getMessage());
	 	}

	 }

	 /*
	  * Add a ConnectionConsumer message.
	  */
	 void addConsumerMessage(MantaBusMessage msg) {
	 	synchronized (consumerMessages) {
	 		this.consumerMessages.add(msg);
	 	}
	 }


	 // called during rollback to redeliver unacked messaged
	 // the messages are added to the beginning of the list.
	 void addConsumerMessages(Collection msgs) {
	 	synchronized (consumerMessages) {
	 		this.consumerMessages.addAll(0,msgs);
	 	}
	 }

	 public boolean hasConsumerMessages() {
		 synchronized (consumerMessages) {
			 return !consumerMessages.isEmpty();
		 }
	 }


	/*
	 * register a listener for a specific service.
	 */
	void registerListener(String regString) throws MantaException {

			manta.subscribeMessageListener(this,regString);
	}

	/*
	 * Registers as a queue-listener for a MessageConsumer
	 *
	 * @param mmc - the MantaMessageConsumer object.
	 */
	void listenToQueue(MantaMessageConsumer mmc) throws JMSException {

		try {
		     this.manta.subscribeToQueue(mmc.theService, this);

		  } catch (Exception e) {
			  throw new InvalidDestinationException("MNJMS0008B : FAILED ON METHOD listenToQueue(). ERROR TEXT : "+e.getMessage());

		  }

	}

	void deregisterFromQueue(MantaMessageConsumer mmc) {
		try {
			this.manta.unsubscribeFromQueue(mmc.theService,this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	protected int sessionAcknowledgementMode = AUTO_ACKNOWLEDGE;
	//protected boolean sessionTransactedMode = false;
	protected boolean isStopped;
	protected boolean isClosed = false;
	protected boolean isClosing = false;
	protected MantaConnection owningConnection;
	protected MessageListener sessionListener = null;
	protected Object lockMonitor = new Object();
	protected static long internalId = 0;
	protected String sessId;
	protected Set heldMessages;
	protected ArrayList consumerMessages;
	protected LinkedHashSet unackedMessages;
	protected Hashtable messageConsumers = new Hashtable();
	protected Hashtable messageProducers = new Hashtable();
	protected Hashtable DurableSubscribers = new Hashtable();
	protected Stage innerQueue;

	/*
	 *
	 * @author Nimo
	 *
	 * A class that encapsulates a message held by this session when committed.
	 */
	class HeldMessage implements Byteable{
		ServiceProducer service;
		MantaMessage msg;

		public HeldMessage(ServiceProducer sp, MantaMessage m) {
			msg = m;
			service = sp;

		}

		public String getByteableName() {

			return "org.mr.api.jms.MantaSession$HeldMessage";
		}

		public void toBytes(ByteableOutputStream out) throws IOException {

			out.writeByteable(service);
			out.writeByteable(msg);
		}

		public Byteable createInstance(ByteableInputStream in) throws IOException {


			HeldMessage hm = new HeldMessage((ServiceProducer) in.readByteable(),(MantaMessage)in.readByteable());
			return hm;
		}

		public void registerToByteableRegistry() {
			ByteableRegistry.registerByteableFactory(getByteableName() , this);

		}
	}

	/* (non-Javadoc)
	 * @see org.mr.core.util.StageHandler#handle(java.lang.Object)
	 */
	public boolean handle(Object event) {

		while (isStopped) {
			// this is a loop in order to prevent the case when the
			// thread is interrupted and released prematurely.
			try {
				synchronized(lockMonitor) {
					// The stop() thread is waiting to hear that the
					// Execution thread is suspened. Notify it.
					lockMonitor.notify();

					lockMonitor.wait();
				}
			}
			catch(InterruptedException ie) {
				
				//PATCH: Don't swallow interrupts
				Thread.currentThread().interrupt();
				
				// can that happen? not taking any risks!
				if (log.isErrorEnabled()) {
					log.error("Error while waiting for the session to resume. ", ie);
				}
			}

			// If the session was closed after it was stopped,
			// return and kill the thread
			if (this.isClosing || this.isClosed) {
				return false;
			}
		}

		// when stopping the session, the stopEvent object was entered
		// into the stage queue. If this is the current object return
		// and dequeue again.
		if (event == stopEvent) {
			return true;
		}

		MantaBusMessage msg = (MantaBusMessage) event;

		if (msg!=null) {

			// this code is for connection consumers to get bus messages
			// to forward to the server session's session.
			if (busListener != null) {
				busListener.onMessage(msg);
				return true;
			}

			String consumer = ((ServiceActor)msg.getRecipient()).getId();
			MantaMessageConsumer destConsumer =
			(MantaMessageConsumer)messageConsumers.get(consumer);

			if (destConsumer != null) {
				synchronized (listenersCount) {
					listenersCount.add();
				}
				synchronized (destConsumer)	{
					if (!destConsumer.isClosed) {
						try {
							destConsumer.feedMessageListener(msg);
						}
						catch (JMSException jmse) {
							log.error("Exception occured in listeners feeding stage", jmse);
						}
					}
				}
				synchronized (listenersCount) {
					listenersCount.remove();
					if (listenersCount.val() == 0) {
						listenersCount.notifyAll();
					}
				}
			}
			else {
				if (log.isDebugEnabled())
					log.debug("A message arrived for a recipient that's closed or not registered on this session.");
			}
		}
		return true;
	}

	/**
	 * converts non mantaRay message to mantaray message
	 * @return
	 * @throws JMSException
	 */
	private MantaMessage fromForeignMsgToManta(Message foreignMessage) throws JMSException {

            MantaMessage mantaResult = null;
            if (foreignMessage instanceof TextMessage) {
            	// covert from Foreign text to Manta Text
                TextMessage mantaTextMsg = (TextMessage) foreignMessage;
                MantaTextMessage msg = new MantaTextMessage();
                msg.setText(mantaTextMsg.getText());
                mantaResult = msg;
            }
            else if (foreignMessage instanceof ObjectMessage)
            {
//            	 covert from Foreign object to Manta object
                ObjectMessage mantaObjectMsg = (ObjectMessage) foreignMessage;
                MantaObjectMessage msg = new MantaObjectMessage();
                msg.setObject(mantaObjectMsg.getObject());
                mantaResult = msg;
            }
            else if (foreignMessage instanceof MapMessage)
            {
//           	 covert from Foreign map to Manta map
                MapMessage mantaMapMsg = (MapMessage) foreignMessage;
                MantaMapMessage msg = new MantaMapMessage();
                for (Enumeration iter = mantaMapMsg.getMapNames(); iter.hasMoreElements();) {
                    String name = iter.nextElement().toString();
                    msg.setObject(name, mantaMapMsg.getObject(name));
                }
                mantaResult = msg;
            }
            else if (foreignMessage instanceof BytesMessage)
            {
//           	 covert from Foreign bytes to Manta bytes
                BytesMessage mantaBytesMsg = (BytesMessage) foreignMessage;
                mantaBytesMsg.reset();
                MantaBytesMessage msg = new MantaBytesMessage();
                try {
                    while(true) {
                        msg.writeByte(mantaBytesMsg.readByte());
                    }
                }
                catch (JMSException e) {
                	// do nothing
                }
                mantaResult = msg;
            }
            else if (foreignMessage instanceof StreamMessage)
            {
                StreamMessage mantaStreamMessage = (StreamMessage) foreignMessage;
//           	 covert from Foreign stream to Manta stream
                mantaStreamMessage.reset();
                MantaStreamMessage mantaStreamMsg = new MantaStreamMessage();
                Object obj = null;
                try {
                    while ((obj = mantaStreamMessage.readObject()) != null) {
                        mantaStreamMsg.writeObject(obj);
                    }
                }
                catch (JMSException e) {
                	// do nothing
                }
                mantaResult = mantaStreamMsg;
            }

            mantaResult.setJMSTimestamp(foreignMessage.getJMSTimestamp());
            mantaResult.setJMSReplyTo(fromForeignDesToManta(foreignMessage.getJMSReplyTo()));
            mantaResult.setJMSMessageID(foreignMessage.getJMSMessageID());
            mantaResult.setJMSCorrelationID(foreignMessage.getJMSCorrelationID());
            mantaResult.setJMSExpiration(foreignMessage.getJMSExpiration());
            mantaResult.setJMSDestination(fromForeignDesToManta(foreignMessage.getJMSDestination()));
            mantaResult.setJMSPriority(foreignMessage.getJMSPriority());
            mantaResult.setJMSDeliveryMode(foreignMessage.getJMSDeliveryMode());

            if (foreignMessage.getJMSRedelivered())
            	mantaResult.flags=mantaResult.flags|MantaMessage.IS_REDELIVERED;

            mantaResult.setJMSPriority(foreignMessage.getJMSPriority());
            Enumeration propertyKeys = foreignMessage.getPropertyNames();

            while( propertyKeys.hasMoreElements())
            {
                String key = propertyKeys.nextElement().toString();
                Object obj = foreignMessage.getObjectProperty(key);
                mantaResult.setObjectProperty(key, obj);
            }
            return mantaResult;
	}

	public boolean isStopped() {
		return isStopped;
	}
	/**
     * @param destination
     * @return a MantaDestination
     * @throws JMSException if an error occurs
     */
    private MantaDestination fromForeignDesToManta(Destination destination) throws JMSException {

    	if (destination==null)
    		return null;

    	if (destination instanceof MantaDestination)
            return (MantaDestination)destination;

        MantaDestination result = null;
        if (destination instanceof TemporaryQueue) {
            result = new MantaTemporaryQueue(((Queue) destination).getQueueName(),null);
        }
        else if (destination instanceof TemporaryTopic) {
            result = new MantaTemporaryTopic(((Topic) destination).getTopicName(),null);
        }
        else if (destination instanceof Queue) {
            result = new MantaQueue(((Queue) destination).getQueueName());
        }
        else if (destination instanceof Topic) {
            result = new MantaTopic(((Topic) destination).getTopicName());
        }

        return result;

    }//fromForeignDesToManta


	public DeliveryListener getDeliveryListener() {
        return deliveryListener;
    }


    public void setDeliveryListener(DeliveryListener deliveryListener) {
        this.deliveryListener = deliveryListener;
    }

	/**
	 * The JCA resource adapter needs to replace the transaction context
	 * of the session with another that operates at the connection level.
	 * @param transactionContext
	 */
	public void setTransactionContext(TransactionContext newContext) {
		if (transactionContext != null) {
			transactionContext.removeSession(this);
		}
		transactionContext = newContext;
		transactionContext.addSession(this);
	}

	/**
	 * Returns the transaction context.
	 * @return
	 */
	public TransactionContext getTransactionContext() {
		return transactionContext;
	}


	protected class Counter {
		int count=0;
		void add() {
			count++;
		}
		void remove() {
			count--;
		}
		int val() {
			return count;
		}

	}

	/**
	 * This interface is used to support the JCA 1.5 standard, to notify
	 * the application server before and after sending a message
	 * to an endpoint (MDB)
	 * @author shaiw
	 */
	public static interface DeliveryListener {
        public void beforeDelivery(MantaSession session, Message msg);
        public void afterDelivery(MantaSession session, Message msg);
    }
}//MantaSession
