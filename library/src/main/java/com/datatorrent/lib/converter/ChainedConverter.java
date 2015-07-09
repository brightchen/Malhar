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

@SuppressWarnings("rawtypes")
public class ChainedConverter<S, T> implements Converter<S, T>
{
  //the implementation doesn't check the match of converter, the client should make sure the converters are match
  private List< Converter > converterChain = new ArrayList< Converter >();
  
  public ChainedConverter(){}
  
  public ChainedConverter( Converter ... converters )
  {
    addConverters(converters);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public T convert(S sourceObj)
  {
    Object source = sourceObj;
    for( Converter converter : converterChain )
    {
      source = converter.convert(source);
    }
    return (T)source;
  }

  public void addConverter( Converter converter )
  {
    converterChain.add(converter);
  }
  
  public void addConverters(Converter ... converters)
  {
    for( Converter converter : converters )
    {
      converterChain.add(converter);
    }
  }
  public void addConverters( List<Converter> converters )
  {
    converterChain.addAll(converters);
  }
}
