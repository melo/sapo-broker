package pt.com.ds;

public interface CacheFiller<K, V>
{
	public V populate(K key);
}
