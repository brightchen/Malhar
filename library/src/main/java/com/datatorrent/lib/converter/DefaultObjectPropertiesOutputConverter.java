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
 * This class convert input object to a Key Value Pair <code>Pair<String,Slice]></code>, 
 * and the Slice is serialized object which converted from input object's properties
 *
 * @param <S>
 */
public class DefaultObjectPropertiesOutputConverter<S> extends DefaultKeyValueOutputConverter<S>
{
  public DefaultObjectPropertiesOutputConverter(){}
  
  public DefaultObjectPropertiesOutputConverter(String keyGetExpression, StreamCodec<S> streamCodec, PropertyInfo... propertyInfos)
  {
    setKeyConverter(createDefaultKeyConverter(keyGetExpression));
    setValueConverter(createDefaultValueConverter(streamCodec, propertyInfos));
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected Converter<S, Slice> createDefaultValueConverter(StreamCodec<S> streamCodec, PropertyInfo... propertyInfos)
  {
    return new ChainedConverter(new ObjectToNameValuePropertiesConverter(propertyInfos), new StreamCodeConverter<S>(streamCodec));
  }
}
