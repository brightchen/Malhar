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
import com.datatorrent.lib.util.PojoUtils.Setter;

@SuppressWarnings("rawtypes")
public class SequentialPropertiesToObjectConverter<T> implements Converter<List, T>
{
  protected List< Setter<T,Object> > setters = new ArrayList< Setter<T,Object> >();
  protected Class<T> targetObjectClass;
  
  public SequentialPropertiesToObjectConverter(){}
  
  public SequentialPropertiesToObjectConverter( Class<T> targetObjectClass )
  {
    setTargetObjectClass(targetObjectClass);
  }

  protected T createEmptyTargetInstance()
  {
    try {
      return targetObjectClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public T convert(List sourceObj)
  {
    return convertTo(sourceObj, createEmptyTargetInstance());
  }
  
  public T convertTo(List sourceObj, T targetValue )
  {
    Iterator valueIter = sourceObj.iterator();
    for( Setter<T,Object> setter : setters )
    {
      if( !valueIter.hasNext() )
        break;
      setter.set(targetValue, valueIter.next() );
    }
    return targetValue;
  }
  

  public void setTargetObjectClass(Class<T> targetObjectClass)
  {
    this.targetObjectClass = targetObjectClass;
  }

  public void setProperties( PropertyInfo ... properties )
  {
    setters.clear();
    addProperties( properties );
  }

  public void addProperties( PropertyInfo ... properties )
  {
    for( PropertyInfo element : properties )
    {
      addProperty( element.getExpression(), element.getType() );
    }
  }

  public void setProperties( Pair<String, Class> ... properties )
  {
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
  
  @SuppressWarnings("unchecked")
  public void addProperty( String expression, Class type )
  {
    Setter<T,Object> setter = PojoUtils.createSetter( targetObjectClass, expression, type );
    setters.add(setter);
  }
 
}
