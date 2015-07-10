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

public class StreamDecodeConverter<T> implements Converter<Slice, T>
{
  private StreamCodec<T> streamCodec;
  
  public StreamDecodeConverter(){}
  
  public StreamDecodeConverter(StreamCodec<T> streamCodec)
  {
    setStreamCodec(streamCodec);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public T convert(Slice sourceObj)
  {
    return (T)streamCodec.fromByteArray(sourceObj);
  }

  public void setStreamCodec(StreamCodec<T> streamCodec)
  {
    this.streamCodec = streamCodec;
  }
}
