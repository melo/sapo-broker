package pt.com.gcs.messaging;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.caudexorigo.ErrorAnalyser;
import org.caudexorigo.Shutdown;
import org.caudexorigo.concurrent.Sleep;
import org.caudexorigo.io.UnsynchByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.bind.ByteArrayBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;

class BDBStorage
{
	private static Logger log = LoggerFactory.getLogger(BDBStorage.class);

	private static final int MAX_DELIVERY_COUNT = 25;

	private Environment env;

	private Database messageDb;

	private String primaryDbName;

	private QueueProcessor queueProcessor;

	private final AtomicBoolean isMarkedForDeletion = new AtomicBoolean(false);

	private AtomicInteger batchCount = new AtomicInteger(0);

	private Object mutex = new Object();

	private Object dbLock = new Object();

	private Queue<Message> _syncConsumerQueue = new ConcurrentLinkedQueue<Message>();

	public BDBStorage(QueueProcessor qp)
	{
		try
		{
			queueProcessor = qp;
			// primaryDbName = MD5.getHashString(queueProcessor.getDestinationName());
			primaryDbName = queueProcessor.getDestinationName();

			env = BDBEnviroment.get();

			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setTransactional(false);
			dbConfig.setAllowCreate(true);
			dbConfig.setSortedDuplicates(false);
			dbConfig.setBtreeComparator(BDBMessageComparator.class);
			messageDb = env.openDatabase(null, primaryDbName, dbConfig);

			log.info("Storage for queue '{}' is ready.", queueProcessor.getDestinationName());
		}
		catch (Throwable t)
		{
			dealWithError(t, false);
			Shutdown.now();
		}
	}

	protected boolean deleteMessage(String msgId)
	{
		if (isMarkedForDeletion.get())
			return false;

		DatabaseEntry key = new DatabaseEntry();
		long k = Long.parseLong(msgId.substring(33));
		LongBinding.longToEntry(k, key);

		try
		{
			OperationStatus op = messageDb.delete(null, key);

			if (op.equals(OperationStatus.SUCCESS))
			{
				return true;
			}
			else
			{
				return false;
			}
		}
		catch (DatabaseException e)
		{
			dealWithError(e, true);
			return false;
		}
	}

	protected long count()
	{
		if (isMarkedForDeletion.get())
			return 0;

		try
		{
			return messageDb.count();
		}
		catch (DatabaseException e)
		{
			dealWithError(e, false);
			return 0;
		}
	}

	protected void insert(Message msg, long sequence, int deliveryCount, boolean localConsumersOnly)
	{
		if (isMarkedForDeletion.get())
			return;

		try
		{
			BDBMessage bdbm = new BDBMessage(msg, sequence, deliveryCount, localConsumersOnly);

			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry data = buildDatabaseEntry(bdbm);

			LongBinding.longToEntry(sequence, key);

			messageDb.put(null, key, data);
		}
		catch (Throwable t)
		{
			dealWithError(t, true);
		}

	}

	private DatabaseEntry buildDatabaseEntry(BDBMessage bdbm) throws IOException
	{
		DatabaseEntry data = new DatabaseEntry();

		UnsynchByteArrayOutputStream bout = new UnsynchByteArrayOutputStream();
		ObjectOutputStream oout = new ObjectOutputStream(bout);
		bdbm.writeExternal(oout);
		oout.flush();

		ByteArrayBinding bab = new ByteArrayBinding();
		byte[] bdata = bout.toByteArray();

		bab.objectToEntry(bdata, data);

		return data;
	}

	protected Message poll()
	{
		if (isMarkedForDeletion.get())
			return null;

		synchronized (mutex)
		{
			if (!_syncConsumerQueue.isEmpty())
			{
				return _syncConsumerQueue.poll();
			}
			else
			{

				synchronized (dbLock)
				{
					Cursor msg_cursor = null;

					try
					{
						msg_cursor = messageDb.openCursor(null, null);

						DatabaseEntry key = new DatabaseEntry();
						DatabaseEntry data = new DatabaseEntry();

						int counter = 0;
						while ((msg_cursor.getNext(key, data, null) == OperationStatus.SUCCESS) && counter < 250)
						{
							byte[] bdata = data.getData();
							BDBMessage bdbm = BDBMessage.fromByteArray(bdata);
							final Message msg = bdbm.getMessage();
							final int deliveryCount = bdbm.getDeliveryCount();							
							final long expiration = msg.getExpiration();
							final long reserved = bdbm.getReserve();
							final long now = System.currentTimeMillis();
							final boolean isReserved = reserved > now ? true : false;
							final boolean safeForPolling = ((reserved==0) && (deliveryCount==0)) ? true : false;

							if (deliveryCount > MAX_DELIVERY_COUNT)
							{
								msg_cursor.delete();
								log.warn("Overdelivered message: '{}' id: '{}'", msg.getDestination(), msg.getMessageId());
								dumpMessage(msg);
							}
							else if (now > expiration)
							{
								msg_cursor.delete();
								log.warn("Expired message: '{}' id: '{}'", msg.getDestination(), msg.getMessageId());
								dumpMessage(msg);
							}
							else
							{
								if (!isReserved || safeForPolling)
								{
									long k = LongBinding.entryToLong(key);
									msg.setMessageId(Message.getBaseMessageId() + k);
									bdbm.setDeliveryCount(deliveryCount + 1);
									bdbm.setReserve(System.currentTimeMillis() + 900000);
									msg_cursor.put(key, buildDatabaseEntry(bdbm));

									_syncConsumerQueue.offer(msg);
								}
							}

							counter++;
						}
					}
					catch (Throwable t)
					{
						dealWithError(t, false);
					}
					finally
					{
						if (msg_cursor != null)
						{
							try
							{
								msg_cursor.close();
							}
							catch (Throwable t)
							{
								dealWithError(t, false);
							}
						}
					}
				}

				return _syncConsumerQueue.poll();
			}
		}
	}

	protected long getLastSequenceValue()
	{
		if (isMarkedForDeletion.get())
			return 0L;

		Cursor msg_cursor = null;
		long seqValue = 0L;

		try
		{
			msg_cursor = messageDb.openCursor(null, null);

			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry data = new DatabaseEntry();

			msg_cursor.getLast(key, data, null);

			seqValue = LongBinding.entryToLong(key);
		}
		catch (Throwable t)
		{
			dealWithError(t, false);
		}
		finally
		{
			if (msg_cursor != null)
			{
				try
				{
					msg_cursor.close();
				}
				catch (Throwable t)
				{
					dealWithError(t, false);
				}
			}
		}
		return seqValue;
	}

	protected void recoverMessages()
	{
		if (isMarkedForDeletion.get())
			return;

		synchronized (dbLock)
		{
			long c0 = System.currentTimeMillis();

			Cursor msg_cursor = null;
			boolean redelivery = ((batchCount.incrementAndGet() % 10) == 0);

			if (redelivery)
			{
				batchCount.set(0);
			}

			try
			{
				msg_cursor = messageDb.openCursor(null, null);

				long c1 = System.currentTimeMillis();
				long d0 = (c1 - c0);

				DatabaseEntry key = new DatabaseEntry();
				DatabaseEntry data = new DatabaseEntry();

				int i0 = 0;
				int j0 = 0;

				while (msg_cursor.getNext(key, data, null) == OperationStatus.SUCCESS)
				{
					if (isMarkedForDeletion.get())
						break;

					byte[] bdata = data.getData();
					BDBMessage bdbm = BDBMessage.fromByteArray(bdata);
					final Message msg = bdbm.getMessage();
					long k = LongBinding.entryToLong(key);
					msg.setMessageId(Message.getBaseMessageId() + k);
					final int deliveryCount = bdbm.getDeliveryCount();
					final boolean localConsumersOnly = bdbm.isLocalConsumersOnly();
					final long reserved = bdbm.getReserve();
					final boolean isReserved = reserved > 0 ? true : false;

					if (!isReserved && ((deliveryCount < 1) || redelivery))
					{
						if (!queueProcessor.isMessageReserved(msg.getMessageId()))
						{

							bdbm.setDeliveryCount(deliveryCount + 1);
							msg_cursor.put(key, buildDatabaseEntry(bdbm));

							long mark = System.currentTimeMillis();

							if (deliveryCount > MAX_DELIVERY_COUNT)
							{
								j0++;
								msg_cursor.delete();
								log.warn("Overdelivered message: '{}' id: '{}'", msg.getDestination(), msg.getMessageId());
								dumpMessage(msg);
							}
							else if (mark > msg.getExpiration())
							{
								j0++;
								msg_cursor.delete();
								log.warn("Expired message: '{}' id: '{}'", msg.getDestination(), msg.getMessageId());
								dumpMessage(msg);
							}
							else
							{
								try
								{
									if (!queueProcessor.forward(msg, localConsumersOnly))
									{
										j0++;
										bdbm.setDeliveryCount(deliveryCount);
										msg_cursor.put(key, buildDatabaseEntry(bdbm));
										dumpMessage(msg);
									}
									else
									{
										i0++;
									}
								}
								catch (Throwable t)
								{
									log.error(t.getMessage());
									break;
								}

							}
						}
					}
				}

				long c2 = System.currentTimeMillis();
				long d1 = (c2 - c1);

				if (log.isDebugEnabled())
				{
					log.debug(String.format("Queue '%s' processing summary; Retrieval time: %s ms; Delivery time: %s ms; Delivered: %s; Failed delivered: %s, Redelivery: %s;", queueProcessor.getDestinationName(), d0, d1, i0, j0, redelivery));
				}
			}
			catch (Throwable t)
			{
				dealWithError(t, false);
			}
			finally
			{
				if (msg_cursor != null)
				{
					try
					{
						msg_cursor.close();
					}
					catch (Throwable t)
					{
						dealWithError(t, false);
					}
				}
			}
		}

	}

	private void dumpMessage(final Message msg)
	{
		if (log.isDebugEnabled())
		{
			log.debug("Could not deliver message. Dump: {}", msg.toString());
		}
	}

	private void dealWithError(Throwable t, boolean rethrow)
	{
		Throwable rt = ErrorAnalyser.findRootCause(t);
		log.error(rt.getMessage(), rt);
		ErrorAnalyser.exitIfOOM(rt);
		if (rethrow)
		{
			throw new RuntimeException(rt);
		}
	}

	protected void deleteQueue()
	{
		isMarkedForDeletion.set(true);
		Sleep.time(2500);
		closeDatabase(messageDb);

		BDBEnviroment.sync();

		removeDatabase(primaryDbName);
	}

	private void closeDatabase(Database db)
	{
		try
		{
			String dbName = db.getDatabaseName();
			log.info("Try to close db '{}'", dbName);
			db.close();
			log.info("Closed db '{}'", dbName);
		}
		catch (Throwable t)
		{
			dealWithError(t, false);
		}
	}

	private void removeDatabase(String dbName)
	{
		int retryCount = 0;

		while (retryCount < 5)
		{
			try
			{
				log.info("Try to remove db '{}'", dbName);
				env.removeDatabase(null, dbName);
				log.info("Removed db '{}'", dbName);
				BDBEnviroment.sync();
				log.info("Storage for queue '{}' was removed", queueProcessor.getDestinationName());

				break;

			}
			catch (Throwable t)
			{
				retryCount++;
				log.error(t.getMessage());
				Sleep.time(2500);
			}
		}

	}

}
