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
import com.datatorrent.common.util.Pair;
import com.datatorrent.netlet.util.Slice;

/**
 * 
 * This class convert input object to a Key Value Pair <code>Pair<String,Slice]></code>, and the value of key(the type
 * is String) is from a property of input object. And the value is serialized input object
 *
 * @param <S>
 * @param <T>
 */
public class DefaultKeyValueOutputConverter<S> extends AbstractObjectToKeyValueConverter<S, Pair<String, Slice>, String, Slice>
{
  public DefaultKeyValueOutputConverter()
  {
  }

  public DefaultKeyValueOutputConverter(Converter<S, String> keyConverter, Converter<S, Slice> valueConverter)
  {
    setKeyConverter(keyConverter);
    setValueConverter(valueConverter);
  }
  
  public DefaultKeyValueOutputConverter(String keyGetExpression, StreamCodec<S> streamCodec)
  {
    setKeyConverter(createDefaultKeyConverter(keyGetExpression));
    setValueConverter(createDefaultValueConverter(streamCodec));
  }
  
  protected Converter<S, String> createDefaultKeyConverter(String keyGetExpression)
  {
    return new ObjectToSinglePropertyConverter<S, String>(keyGetExpression, String.class);
  }
  
  protected Converter<S, Slice> createDefaultValueConverter(StreamCodec<S> streamCodec)
  {
    return new StreamCodeConverter<S>(streamCodec);
  }

  @Override
  protected Pair<String, Slice> createTargetObject(String key, Slice value)
  {
    return new Pair<String, Slice>( key, value );
  }

}
