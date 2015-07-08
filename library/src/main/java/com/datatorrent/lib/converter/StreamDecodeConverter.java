package com.datatorrent.lib.converter;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.netlet.util.Slice;

public class StreamDecodeConverter<T> implements Converter<Slice, T>
{
  private StreamCodec<T> streamCodec;
  
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
