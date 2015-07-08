package com.datatorrent.lib.converter;


public abstract class AbstractObjectToKeyValueConverter<S, T, K, V> implements Converter<S, T>
{
  protected Converter<S, K> keyConverter;
  protected Converter<S, V> valueConverter;

  @Override
  public T convert(S sourceObj)
  {
    return createTargetObject( keyConverter.convert(sourceObj), valueConverter.convert(sourceObj));
  }

  protected abstract T createTargetObject( K key, V value);
  

  public void setKeyConverter(Converter<S, K> keyConverter)
  {
    this.keyConverter = keyConverter;
  }

  public void setValueConverter(Converter<S, V> valueConverter)
  {
    this.valueConverter = valueConverter;
  }

}
