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

import com.datatorrent.api.StreamCodec;
import com.datatorrent.netlet.util.Slice;

/**
 * 
 * This class convert input Key Value Pair <code>Pair<String,Slice]></code>, in which Slice is serialized object
 * to an object 
 *
 * @param <T>
 */
public class DefaultObjectPropertiesInputConverter<T> extends DefaultKeyValueInputConverter<T>
{
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public DefaultObjectPropertiesInputConverter(String keySetExpression, Class<T> targetObjectClass, StreamCodec<T> streamCodec, PropertyInfo ... properties)
  {
    setKeyConverter(new SinglePropertyToObjectConverter(keySetExpression));
    setValueConverter(createDefaultValueConverter(targetObjectClass, streamCodec, properties));
  }
  
  protected Converter<Slice, T> createDefaultValueConverter(Class<T> targetObjectClass, StreamCodec<T> streamCodec, PropertyInfo... propertyInfos)
  {
    return new ChainedConverter<Slice, T>(new StreamDecodeConverter<T>(streamCodec), 
        new NameValuePropertiesToObjectConverter<T>(targetObjectClass,propertyInfos));
  }
}
