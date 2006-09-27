package pt.com.ds;


import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


public class MultiValueHashMap<K, V> extends HashMap<K, List<V>>
{

    private static final long serialVersionUID = 10596826974616419L;

    public MultiValueHashMap()
    {}

    public void put( K key, V value )
    {
        List<V> elems = get( key );
        if ( elems == null )
        {
            elems = new LinkedList<V>();
            put( key, elems );
        }
        elems.add( value );
    }


    public int getValueCount()
    {
        int size = 0;
        for ( List<V> list : values() )
        {
            size += list.size();
        }
        return size;
    }


    public int getValueCount( K key )
    {
        List<V> list = get( key );
        if ( list == null )
        {
            return 0;
        }
        return list.size();
    }


    public int getKeyCount()
    {
        return size();
    }
}