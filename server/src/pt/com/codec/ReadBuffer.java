package pt.com.codec;

import pt.com.io.ByteArrayOutputStream;

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
	
	protected void allocate()
	{
		packetbuf = new ByteArrayOutputStream();
	}
}