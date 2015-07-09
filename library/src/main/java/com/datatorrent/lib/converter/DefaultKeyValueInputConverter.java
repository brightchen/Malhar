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
 * Assume the input is a pair of key to value Pair<String,Slice>, and the output is the object
 * please see class DefaultKeyValueOutputConverter
 * 
 */
public class DefaultKeyValueInputConverter<T> implements Converter<Pair<String, Slice>, T>
{
  protected ShareableConverter<String, T> keyConverter;
  protected Converter<Slice, T> valueConverter;

  public DefaultKeyValueInputConverter()
  {
  }

  public DefaultKeyValueInputConverter(String keySetExpression, StreamCodec<T> streamCodec)
  {
    setKeyConverter(createDefaultKeySharableConverter(keySetExpression));
    setValueConverter(createDefaultValueConverter(streamCodec));
  }
  
  protected ShareableConverter<String, T> createDefaultKeySharableConverter(String keySetExpression)
  {
    return new SinglePropertyToObjectConverter<String, T>(keySetExpression);
  }
  
  protected Converter<Slice, T> createDefaultValueConverter(StreamCodec<T> streamCodec)
  {
    return new StreamDecodeConverter<T>(streamCodec);
  }

  @Override
  public T convert(Pair<String, Slice> sourceObj)
  {
    T targetObj = valueConverter.convert(sourceObj.getSecond());
    keyConverter.convertTo(sourceObj.getFirst(), targetObj);

    return targetObj;
  }

  public void setKeyConverter(ShareableConverter<String, T> keyConverter)
  {
    this.keyConverter = keyConverter;
  }

  public void setValueConverter(Converter<Slice, T> valueConverter)
  {
    this.valueConverter = valueConverter;
  }

}