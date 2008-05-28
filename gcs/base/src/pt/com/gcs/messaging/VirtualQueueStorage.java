package pt.com.gcs.messaging;

import java.util.ArrayList;

import org.caudexorigo.ErrorAnalyser;
import org.caudexorigo.Shutdown;
import org.caudexorigo.cryto.MD5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class VirtualQueueStorage
{
	private static Logger log = LoggerFactory.getLogger(BDBStorage.class);

	private static final VirtualQueueStorage instance = new VirtualQueueStorage();

	private Environment env;

	private Database vqStorage;

	private String dbName;

	private VirtualQueueStorage()
	{
		try
		{
			env = BDBEnviroment.get();
			dbName = MD5.getHashString("VirtualQueueStorage");

			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setTransactional(true);
			dbConfig.setAllowCreate(true);
			dbConfig.setSortedDuplicates(false);
			vqStorage = env.openDatabase(null, dbName, dbConfig);

			log.info("VirtualQueueStorage is ready.");
		}
		catch (Throwable t)
		{
			Throwable rt = ErrorAnalyser.findRootCause(t);
			log.error(rt.getMessage(), rt);
			Shutdown.now();
		}
	}

	private void i_saveVirtualQueue(String queueName)
	{
		try
		{
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry data = new DatabaseEntry();

			StringBinding.stringToEntry(queueName, key);
			StringBinding.stringToEntry(queueName, data);

			Transaction txn = env.beginTransaction(null, null);
			OperationStatus ops = vqStorage.put(txn, key, data);
			System.out.println("VirtualQueueStorage.i_saveVirtualQueue.ops: " + ops.toString());
			txn.commitSync();
		}
		catch (Throwable t)
		{
			Throwable rt = ErrorAnalyser.findRootCause(t);
			log.error(rt.getMessage(), rt);
		}

	}

	private void i_deleteVirtualQueue(String queueName)
	{
		try
		{
			DatabaseEntry key = new DatabaseEntry();

			StringBinding.stringToEntry(queueName, key);

			Transaction txn = env.beginTransaction(null, null);
			vqStorage.delete(txn, key);
			txn.commitSync();
		}
		catch (Throwable t)
		{
			Throwable rt = ErrorAnalyser.findRootCause(t);
			log.error(rt.getMessage(), rt);
		}

	}

	public static void saveVirtualQueue(String queueName)
	{
		System.out.println("VirtualQueueStorage.saveVirtualQueue(): " + queueName);
		instance.i_saveVirtualQueue(queueName);
	}

	public static void deleteVirtualQueue(String queueName)
	{
		instance.i_deleteVirtualQueue(queueName);
	}

	public String[] i_getQueuesNames()
	{
		Cursor cursor = null;
		try
		{
			ArrayList<String> lst = new ArrayList<String>();

			cursor = vqStorage.openCursor(null, null);

			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry data = new DatabaseEntry();

			while (cursor.getNext(key, data, null) == OperationStatus.SUCCESS)
			{

				String qname = StringBinding.entryToString(data);
				System.out.println("VirtualQueueStorage.i_getQueuesNames.qname: " + qname);
				lst.add(qname);
			}

			return lst.toArray(new String[lst.size()]);
		}
		catch (Throwable t)
		{
			Throwable rt = ErrorAnalyser.findRootCause(t);
			log.error(rt.getMessage(), rt);
			return new String[0];
		}
		finally
		{
			if (cursor != null)
			{
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
					Throwable rt = ErrorAnalyser.findRootCause(t);
					log.error(rt.getMessage(), rt);
				}
			}
		}
	}

	public static String[] getQueuesNames()
	{
		return instance.i_getQueuesNames();
	}
}
