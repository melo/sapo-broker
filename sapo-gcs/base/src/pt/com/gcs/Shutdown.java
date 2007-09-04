package pt.com.gcs;

public class Shutdown
{
	public static void main(String[] args)
	{
		//NOP: ServiceWrapper artifact
	}

	public static void now()
	{
		System.out.println("\nExiting... ");		
		while (true)
		{
			System.exit(-1);
		}
	}
}
