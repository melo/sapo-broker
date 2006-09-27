package pt.com.broker;

import java.util.HashMap;
import java.util.Map;

public class AckModeLookup
{
	private static final AckModeLookup instance = new AckModeLookup();

	private final Map<Integer, String> _vt = new HashMap<Integer, String>();

	private final Map<String, Integer> _tv = new HashMap<String, Integer>();

	private AckModeLookup()
	{
		_vt.put(1, "AUTO");
		_vt.put(2, "CLIENT");
		_tv.put("AUTO", 1);
		_tv.put("CLIENT", 2);
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
