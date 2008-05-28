package pt.com.gcs.conf;

public class XsdValidationResult
{
	private final boolean _valid;

	private final String _message;

	public XsdValidationResult(boolean valid, String message)
	{
		_valid = valid;
		_message = message;
	}

	public boolean isValid()
	{
		return _valid;
	}

	public String getMessage()
	{
		return _message;
	}

}
