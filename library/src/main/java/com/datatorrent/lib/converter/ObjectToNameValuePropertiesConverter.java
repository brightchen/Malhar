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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.datatorrent.lib.util.PojoUtils.Getter;

public class ObjectToNameValuePropertiesConverter<S> extends ObjectPropertiesHolder<S> implements Converter<S, Map<String, Object>>, ShareableConverter<S, Map<String, Object>>
{
  public ObjectToNameValuePropertiesConverter(){}
  
  public ObjectToNameValuePropertiesConverter(PropertyInfo... propertyInfos)
  {
    setProperties(propertyInfos);
  }

  @Override
  public Map<String, Object> convert(S sourceObj)
  {
    Map<String, Object> propertyValue = new HashMap<String, Object>();
    return convertTo(sourceObj, propertyValue);
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> convertTo(S sourceObj, Map<String, Object> propertyValues)
  {
    if( getters == null || getters.isEmpty() )
      generateGetters((Class<S>)sourceObj.getClass());
    Iterator<Getter<S, Object>> getterIter = getters.iterator();
    Iterator<PropertyInfo> propertyIter = propertyInfos.iterator();
    
    while( getterIter.hasNext() )
    {
      propertyValues.put(propertyIter.next().getName(), getterIter.next().get(sourceObj) );
    }
    
    return propertyValues;
  }
}
