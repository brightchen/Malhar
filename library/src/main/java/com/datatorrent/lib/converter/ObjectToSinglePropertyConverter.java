package com.datatorrent.lib.converter;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * This interface get a property from the source object as the target object
 *
 * @param <S>
 * @param <T>
 */
public class ObjectToSinglePropertyConverter<S, T> implements Converter<S, T>
{
  private String getExpression;
  private Class<T> targetClass;
  protected Getter<S, T> getter;

  public ObjectToSinglePropertyConverter()
  {
  }

  public ObjectToSinglePropertyConverter(String getExpression, Class<T> targetClass)
  {
    setProperty(getExpression, targetClass);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T convert(S sourceObj)
  {
    if (getter == null)
      getter = createGetter((Class<S>) sourceObj.getClass(), targetClass);
    return getter.get(sourceObj);
  }

  protected Getter<S, T> createGetter(Class<S> sourceObjectClass, Class<T> convertToClass)
  {
    return PojoUtils.createGetter(sourceObjectClass, getExpression, convertToClass);
  }

  public void setProperty(String getExpression, Class<T> targetClass)
  {
    this.getExpression = getExpression;
    this.targetClass = targetClass;
  }

}
