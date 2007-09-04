package pt.com.broker.messaging;

public class Notify
{
	public String destinationName;

	public String destinationType;

	public AcknowledgeMode acknowledgeMode;

	public Notify()
	{
		destinationName = "";
		destinationType = "";
		acknowledgeMode = AcknowledgeMode.AUTO;
	}

	@Override
	public int hashCode()
	{
		final int PRIME = 31;
		int result = 1;
		result = PRIME * result + ((acknowledgeMode == null) ? 0 : acknowledgeMode.getValue());
		result = PRIME * result + ((destinationName == null) ? 0 : destinationName.hashCode());
		result = PRIME * result + ((destinationType == null) ? 0 : destinationType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final Notify other = (Notify) obj;
		if (acknowledgeMode == null)
		{
			if (other.acknowledgeMode != null)
				return false;
		}
		else if (acknowledgeMode != other.acknowledgeMode)
			return false;
		if (destinationName == null)
		{
			if (other.destinationName != null)
				return false;
		}
		else if (!destinationName.equals(other.destinationName))
			return false;
		if (destinationType == null)
		{
			if (other.destinationType != null)
				return false;
		}
		else if (!destinationType.equals(other.destinationType))
			return false;
		return true;
	}

}
