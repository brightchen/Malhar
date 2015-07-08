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
