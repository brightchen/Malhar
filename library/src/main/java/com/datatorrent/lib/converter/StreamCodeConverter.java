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

public class StreamCodeConverter<S> implements Converter<S, Slice>
{
  private StreamCodec<S> streamCodec;
  
  public StreamCodeConverter(){}
  
  public StreamCodeConverter( StreamCodec<S> streamCodec )
  {
    setStreamCodec(streamCodec);
  }
  
  @Override
  public Slice convert(S sourceObj)
  {
    return streamCodec.toByteArray(sourceObj);
  }

  public void setStreamCodec(StreamCodec<S> streamCodec)
  {
    this.streamCodec = streamCodec;
  }

}
