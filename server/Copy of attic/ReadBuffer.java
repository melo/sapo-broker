package pt.com.codec;

public class ReadBuffer
{
	protected int readLenght;

	protected int lenght;

	protected ByteArrayOutputStream packetbuf;

	public ReadBuffer()
	{
		readLenght = 0;
		lenght = 0;
	}
	
	protected void allocate(int size)
	{
		packetbuf = new ByteArrayOutputStream(size);
	}
}