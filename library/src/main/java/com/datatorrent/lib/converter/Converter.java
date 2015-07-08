package com.datatorrent.lib.converter;

public interface Converter<S, T>
{
  public T convert(S sourceObj);
}
