package com.datatorrent.lib.converter;


/**
 * 
 * This interface provides methods to convert Object to another Object which represented by its properties
 *
 * @param <S> the type of the object
 * @param <T> the type of the object converted to
 */
@SuppressWarnings("rawtypes")
public interface ObjectPropertiesConverter< S, T >
{
  public T fromOriginObject( S obj );
  
  /**
   * This method same as <code>fromObject( Object obj )</code>. 
   * just give a chance to reuse the propertyValues object, or use sub-type of T
   * @param value
   * @param propertyValues
   * @return
   */
  public T fromOriginObject( S value, T propertyValues );
  
  public S toOriginObject( T propertyValues );
  
  public S toOriginObject( T propertyValues, S value );
  
  
}
