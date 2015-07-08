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
public class DefaultKeyValueOutputConverter<S> extends AbstractObjectToKeyValueConverter<S, Pair<String, Slice>, String, Slice>
{
  public DefaultKeyValueOutputConverter()
  {
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public DefaultKeyValueOutputConverter(String keyGetExpression, StreamCodec<S> streamCodec)
  {
    setKeyConverter(new ObjectToSinglePropertyConverter(keyGetExpression, String.class));
    setValueConverter(new StreamCodeConverter(streamCodec));
  }

  @Override
  protected Pair<String, Slice> createTargetObject(String key, Slice value)
  {
    return new Pair<String, Slice>( key, value );
  }

}
