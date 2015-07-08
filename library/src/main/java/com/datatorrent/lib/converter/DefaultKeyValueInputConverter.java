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

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public DefaultKeyValueInputConverter(String setExpression, StreamCodec<T> streamCodec)
  {
    setKeyConverter(new SinglePropertyToObjectConverter(setExpression));
    setValueConverter(new StreamDecodeConverter(streamCodec));
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