package pt.com.io;

import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.ByteBufferAllocator;
import org.apache.mina.common.support.BaseByteBuffer;

public class UnsynchByteBufferAllocator implements ByteBufferAllocator
{
	private static final int MINIMUM_CAPACITY = 1;

	public UnsynchByteBufferAllocator()
	{
	}

	public ByteBuffer allocate(int capacity, boolean direct)
	{
        java.nio.ByteBuffer nioBuffer;
        nioBuffer = java.nio.ByteBuffer.allocate( capacity );
        return new SimpleByteBuffer( nioBuffer );
	}

    public ByteBuffer wrap( java.nio.ByteBuffer nioBuffer )
    {
        return new SimpleByteBuffer( nioBuffer );
    }

    public void dispose()
    {
    }

    private static class SimpleByteBuffer extends BaseByteBuffer
	    {
	        private java.nio.ByteBuffer buf;
	        private AtomicInteger refCount = new AtomicInteger(1);

	        protected SimpleByteBuffer( java.nio.ByteBuffer buf )
	        {
	            this.buf = buf;
	            buf.order( ByteOrder.BIG_ENDIAN );
	            refCount.set(1);
	        }

	        
			public void acquire()
			{
				if (refCount.incrementAndGet() <= 1)
				{
					throw new IllegalStateException("Already released buffer.");
				}	
			}	        

			public void release()
			{
				if (refCount.decrementAndGet() <= -1)
				{
					refCount.set(0);
					throw new IllegalStateException("Already released buffer.  You released the buffer too many times.");
				}
			}


	        public java.nio.ByteBuffer buf()
	        {
	            return buf;
	        }
	        
	        public boolean isPooled()
	        {
	            return false;
	        }
	        
	        public void setPooled( boolean pooled )
	        {
	        }

	        protected void capacity0( int requestedCapacity )
	        {
	            int newCapacity = MINIMUM_CAPACITY;
	            while( newCapacity < requestedCapacity )
	            {
	                newCapacity <<= 1;
	            }
	            
	            java.nio.ByteBuffer oldBuf = this.buf;
	            java.nio.ByteBuffer newBuf;
	            if( isDirect() )
	            {
	                newBuf = java.nio.ByteBuffer.allocateDirect( newCapacity );
	            }
	            else
	            {
	                newBuf = java.nio.ByteBuffer.allocate( newCapacity );
	            }

	            newBuf.clear();
	            oldBuf.clear();
	            newBuf.put( oldBuf );
	            this.buf = newBuf;
	        }

	        public ByteBuffer duplicate() {
	            return new SimpleByteBuffer( this.buf.duplicate() );
	        }

	        public ByteBuffer slice() {
	            return new SimpleByteBuffer( this.buf.slice() );
	        }

	        public ByteBuffer asReadOnlyBuffer() {
	            return new SimpleByteBuffer( this.buf.asReadOnlyBuffer() );
	        }

	        public byte[] array()
	        {
	            return buf.array();
	        }
	        
	        public int arrayOffset()
	        {
	            return buf.arrayOffset();
	        }
	    }
}
