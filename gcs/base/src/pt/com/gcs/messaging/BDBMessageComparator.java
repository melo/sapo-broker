package pt.com.gcs.messaging;

import java.util.Comparator;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.DatabaseEntry;

public class BDBMessageComparator implements Comparator<byte[]>
{

	@Override
	public int compare(byte[] bkey1, byte[] bkey2)
	{
		DatabaseEntry arg1 = new DatabaseEntry(bkey1);
		DatabaseEntry arg2 = new DatabaseEntry(bkey2);

		long l1 = LongBinding.entryToLong(arg1);
		long l2 = LongBinding.entryToLong(arg2);

		if (l1 < l2)
		{
			return -1;
		}
		else if (l1 > l2)
		{
			return 1;
		}
		else
		{
			return 0;
		}
	}
}
