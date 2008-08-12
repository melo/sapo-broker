package pt.com.broker.messaging;

public class Unsubscribe
{
	public String actionId;
	
	public String destinationName;

	public String destinationType;

	public Unsubscribe()
	{
		destinationName = "";
		destinationType = "";
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((destinationName == null) ? 0 : destinationName.hashCode());
		result = prime * result + ((destinationType == null) ? 0 : destinationType.hashCode());
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
		final Unsubscribe other = (Unsubscribe) obj;
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
