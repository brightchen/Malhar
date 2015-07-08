package com.datatorrent.lib.converter;

/**
 * This interface use input targetObj to output the result instead of create an new instance. 
 *
 * @param <S>
 * @param <T>
 */
public interface ShareableConverter<S, T>
{
  public T convertTo(S sourceObj, T targetObj);
}
