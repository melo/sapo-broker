package pt.com.gcs.messaging;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

public class BDBMessageKeyCreator implements SecondaryKeyCreator
{
	@Override
	public boolean createSecondaryKey(SecondaryDatabase secDb, DatabaseEntry keyEntry, DatabaseEntry dataEntry, DatabaseEntry resultEntry) throws DatabaseException
	{
		byte[] bdata = dataEntry.getData();
		BDBMessage bdbm;
		try
		{
			bdbm = BDBMessage.fromByteArray(bdata);
		}
		catch (Throwable t)
		{
			t.printStackTrace();
			return false;
		}

		String msgId = bdbm.getMessage().getMessageId();
		StringBinding.stringToEntry(msgId, resultEntry);

		return true;
	}
}
