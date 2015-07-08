package com.datatorrent.lib.converter;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("rawtypes")
public class ChainedConverter<S, T> implements Converter<S, T>
{
  //the implementation doesn't check the match of converter, the client should make sure the converters are match
  private List< Converter > converterChain = new ArrayList< Converter >();
  
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
  
  public void addConverters( List<Converter> converters )
  {
    converterChain.addAll(converters);
  }
}
