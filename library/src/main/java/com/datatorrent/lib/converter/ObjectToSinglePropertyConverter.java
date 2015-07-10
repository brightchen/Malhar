/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
