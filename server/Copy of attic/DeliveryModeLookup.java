package pt.com.broker;

import java.util.HashMap;
import java.util.Map;

public class DeliveryModeLookup
{
	private static final DeliveryModeLookup instance = new DeliveryModeLookup();

	private final Map<Integer, String> _vt = new HashMap<Integer, String>();

	private final Map<String, Integer> _tv = new HashMap<String, Integer>();

	private DeliveryModeLookup()
	{
		_vt.put(1, "TRANSIENT");
		_vt.put(2, "PERSISTENT");
		_tv.put("TRANSIENT", 1);
		_tv.put("PERSISTENT", 2);
	}

	public static String getString(int value)
	{
		return instance._vt.get(value);
	}

	public static int getValue(String text)
	{
		return instance._tv.get(text);
	}

}
