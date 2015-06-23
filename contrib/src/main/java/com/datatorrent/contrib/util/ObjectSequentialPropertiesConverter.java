package com.datatorrent.contrib.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datatorrent.common.util.Pair;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.Setter;

/**
 * 
 * This implementation use the sequence to match the properties.
 *
 * @param <V>
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ObjectSequentialPropertiesConverter<V> implements ObjectPropertiesConverter<V, List>
{
  private List< Getter<V,Object> > getters = new ArrayList< Getter<V,Object> >();
  private List< Setter<V,Object> > setters = new ArrayList< Setter<V,Object> >();
  
  private Class<V> objectClass;
  public ObjectSequentialPropertiesConverter( Class<V> objectClass )
  {
    this.objectClass = objectClass;
  }
  

  @Override
  public List fromObject(V value, List propertyValues )
  {
    propertyValues.clear();
    for( Getter<V,Object> getter : getters )
    {
      propertyValues.add( getter.get(value) );
    }
    return propertyValues;
  }
  
  @Override
  public List fromObject(V obj)
  {
    List< Object > propertyValue = new ArrayList< Object >();
    return fromObject( obj, propertyValue );
  }

  @Override
  public V toObject(List propertyValues)
  {
    try
    {
      return toObject( propertyValues, objectClass.newInstance() );
    }
    catch( Exception e )
    {
      throw new RuntimeException( e );
    }
    
  }
  
  public V toObject( List propertyValues, V value )
  {
    Iterator valueIter = propertyValues.iterator();
    for( Setter<V,Object> setter : setters )
    {
      if( !valueIter.hasNext() )
        break;
      setter.set(value, valueIter.next() );
    }
    return value;
  }
  
  public void setProperties( List< Pair<String, Class> > properties )
  {
    getters.clear();
    setters.clear();
    addProperties( properties );
  }
  
  public void addProperties( List< Pair<String, Class> > properties )
  {
    for( Pair<String,Class> element : properties )
    {
      addProperty( element.getFirst(), element.getSecond() );
    }
  }
  
  public void addProperty( String expression, Class type )
  {
    {
      Getter<V,Object> getter = PojoUtils.createGetter( objectClass, expression, type );
      getters.add(getter);
    }
    
    {
      Setter<V,Object> setter = PojoUtils.createSetter( objectClass, expression, type );
      setters.add(setter);
    }
  }
}
