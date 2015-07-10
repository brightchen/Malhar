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


public abstract class AbstractObjectToKeyValueConverter<S, T, K, V> implements Converter<S, T>
{
  protected Converter<S, K> keyConverter;
  protected Converter<S, V> valueConverter;

  @Override
  public T convert(S sourceObj)
  {
    return createTargetObject( keyConverter.convert(sourceObj), valueConverter.convert(sourceObj));
  }

  protected abstract T createTargetObject( K key, V value);
  

  public void setKeyConverter(Converter<S, K> keyConverter)
  {
    this.keyConverter = keyConverter;
  }

  public void setValueConverter(Converter<S, V> valueConverter)
  {
    this.valueConverter = valueConverter;
  }

}
