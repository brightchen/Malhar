package com.datatorrent.contrib.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.Setter;

public class ObjectKeyValuePropertiesConverter<V> implements ObjectPropertiesConverter<V, Map<String, Object> >
{
  private Map<String, Getter<V,Object> > getterMap = new HashMap<String, Getter<V,Object> >();
  private Map<String, Setter<V,Object> > setterMap = new HashMap<String, Setter<V,Object> >();
  
  private Class<V> objectClass;
 
  public ObjectKeyValuePropertiesConverter( Class<V> objectClass )
  {
    this.objectClass = objectClass;
  }
  
  @Override
  public Map<String, Object> fromObject(V value)
  {
    Map<String, Object> propertyValue = new HashMap<String, Object>();
    return fromObject( value, propertyValue );
  }

  @Override
  public Map<String, Object> fromObject(V value, Map<String, Object> propertyValues)
  {
    propertyValues.clear();
    for( Map.Entry<String, Getter<V,Object> > entry : getterMap.entrySet() )
    {
      propertyValues.put( entry.getKey(), entry.getValue().get(value) );
    }
    return propertyValues;
  }

  @Override
  public V toObject(Map<String, Object> propertyValues)
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

  @Override
  public V toObject(Map<String, Object> propertyValues, V value )
  {
    for( Map.Entry<String, Setter<V,Object> > entry : setterMap.entrySet() )
    {
      entry.getValue().set( value, propertyValues.get( entry.getKey() ) );
    }
    return value;
  }
  
  public void setProperties( Collection< PropertyInfo > propertyInfos )
  {
    getterMap.clear();
    setterMap.clear();
    addProperties( propertyInfos );
  }
  
  public void addProperties( Collection< PropertyInfo > propertyInfos )
  {
    for( PropertyInfo propertyInfo : propertyInfos )
    {
      addProperty( propertyInfo.name, propertyInfo.expression, propertyInfo.type );
    }
  }
  
  public void setProperties( PropertyInfo ... propertyInfos )
  {
    getterMap.clear();
    setterMap.clear();
    addProperties( propertyInfos );
  }
  
  public void addProperties( PropertyInfo ... propertyInfos )
  {
    for( PropertyInfo propertyInfo : propertyInfos )
    {
      addProperty( propertyInfo.name, propertyInfo.expression, propertyInfo.type );
    }
  }
  
  public void addProperty( String name, String expression, Class type )
  {
    {
      Getter<V,Object> getter = PojoUtils.createGetter( objectClass, expression, type );
      getterMap.put( name, getter);
    }
    
    {
      Setter<V,Object> setter = PojoUtils.createSetter( objectClass, expression, type );
      setterMap.put( name, setter );
    }
  }
  
}
