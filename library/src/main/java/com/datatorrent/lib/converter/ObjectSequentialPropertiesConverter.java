/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.converter;

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
  
  private Class<V> sourceObjectClass;
  
  public ObjectSequentialPropertiesConverter(){}
  
  public ObjectSequentialPropertiesConverter( Class<V> sourceObjectClass )
  {
    this.setSourceObjectClass(sourceObjectClass);
  }
  
  public ObjectSequentialPropertiesConverter( Class<V> objectClass, PropertyInfo ... properties )
  {
    setSourceObjectClass(objectClass);
    setProperties(properties);
  }

  @Override
  public List fromOriginObject(V value, List propertyValues )
  {
    propertyValues.clear();
    for( Getter<V,Object> getter : getters )
    {
      propertyValues.add( getter.get(value) );
    }
    return propertyValues;
  }
  
  @Override
  public List fromOriginObject(V obj)
  {
    List< Object > propertyValue = new ArrayList< Object >();
    return fromOriginObject( obj, propertyValue );
  }

  @Override
  public V toOriginObject(List propertyValues)
  {
    try
    {
      return toOriginObject( propertyValues, sourceObjectClass.newInstance() );
    }
    catch( Exception e )
    {
      throw new RuntimeException( e );
    }
    
  }
  
  public V toOriginObject( List propertyValues, V value )
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
  

  public void setProperties( PropertyInfo ... properties )
  {
    getters.clear();
    setters.clear();
    addProperties( properties );
  }

  public void addProperties( PropertyInfo ... properties )
  {
    for( PropertyInfo element : properties )
    {
      addProperty( element.expression, element.type );
    }
  }
  

  public void setSourceObjectClass(Class<V> sourceObjectClass)
  {
    this.sourceObjectClass = sourceObjectClass;
  }

  public void setProperties(List<PropertyInfo> propertyInfos)
  {
    getters.clear();
    addProperties(propertyInfos);
  }

  public void addProperties(List<PropertyInfo> propertyInfos)
  {
    for (PropertyInfo propertyInfo : propertyInfos) {
      addProperty(propertyInfo.expression, propertyInfo.type);
    }
  }
  
  public void setProperties( Pair<String, Class> ... properties )
  {
    getters.clear();
    setters.clear();
    addProperties( properties );
  }
  
  public void addProperties( Pair<String, Class> ... properties )
  {
    for( Pair<String,Class> element : properties )
    {
      addProperty( element.getFirst(), element.getSecond() );
    }
  }
  
  
  public void addProperty( String expression, Class type )
  {
    {
      Getter<V,Object> getter = PojoUtils.createGetter( sourceObjectClass, expression, type );
      getters.add(getter);
    }
    
    {
      Setter<V,Object> setter = PojoUtils.createSetter( sourceObjectClass, expression, type );
      setters.add(setter);
    }
  }
}
