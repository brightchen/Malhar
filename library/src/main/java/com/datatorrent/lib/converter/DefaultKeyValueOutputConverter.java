package com.datatorrent.lib.converter;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.Pair;
import com.datatorrent.netlet.util.Slice;

/**
 * 
 * This class convert input object to a Key Value Pair <code>Pair<String,Slice]></code>, and the value of key(the type
 * is String) is from a property of input object. And the value is serialized input object
 *
 * @param <S>
 * @param <T>
 */
public class DefaultKeyValueOutputConverter<S> implements Converter<S, Pair<String, Slice>>
{
  protected Converter<S, String> keyConverter;
  protected Converter<S, Slice> valueConverter;

  public DefaultKeyValueOutputConverter()
  {
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public DefaultKeyValueOutputConverter(String getExpression, StreamCodec<S> streamCodec)
  {
    setKeyConverter(new ObjectToSinglePropertyConverter(getExpression, Pair.class));
    setValueConverter(new StreamCodeConverter(streamCodec));
  }

  @Override
  public Pair<String, Slice> convert(S sourceObj)
  {
    return new Pair<String, Slice>(keyConverter.convert(sourceObj), valueConverter.convert(sourceObj));
  }

  public void setKeyConverter(Converter<S, String> keyConverter)
  {
    this.keyConverter = keyConverter;
  }

  public void setValueConverter(Converter<S, Slice> valueConverter)
  {
    this.valueConverter = valueConverter;
  }

}
