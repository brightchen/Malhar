package com.datatorrent.contrib.util;

/**
 * 
 * This interface provides methods to convert Object to another Object which represented by its properties
 *
 * @param <V> the type of the object
 * @param <P> the type of the object which keep properties
 */
public interface ObjectPropertiesConverter< V, P>
{
  public P fromObject( V obj );
  
  /**
   * This method same as <code>fromObject( Object obj )</code>. 
   * just give a chance to reuse the propertyValues object, or use sub-type of T
   * @param value
   * @param propertyValues
   * @return
   */
  public P fromObject( V value, P propertyValues );
  
  public V toObject( P propertyValues );
  
  public V toObject( P propertyValues, V value );
}
