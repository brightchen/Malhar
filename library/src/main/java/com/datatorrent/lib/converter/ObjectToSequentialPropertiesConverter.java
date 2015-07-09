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
import java.util.List;

import com.datatorrent.lib.util.PojoUtils.Getter;

@SuppressWarnings("rawtypes")
public class ObjectToSequentialPropertiesConverter<S> extends ObjectPropertiesHolder<S> implements Converter<S, List>, ShareableConverter<S, List>
{
  public ObjectToSequentialPropertiesConverter(){}

  public ObjectToSequentialPropertiesConverter(PropertyInfo ... properties)
  {
    addProperties(properties);
  }

  @Override
  public List convert(S sourceObj)
  {
    List< Object > propertyValue = new ArrayList< Object >();
    return convertTo( sourceObj, propertyValue );
  }
  
  @SuppressWarnings("unchecked")
  public List convertTo(S sourceObj, List propertyValues )
  {
    if( getters == null || getters.isEmpty() )
      generateGetters((Class<S>)sourceObj.getClass());
    propertyValues.clear();
    for( Getter<S,Object> getter : getters )
    {
      propertyValues.add( getter.get(sourceObj) );
    }
    return propertyValues;
  }
  
}
