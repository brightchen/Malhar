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
import com.datatorrent.lib.util.PojoUtils.Setter;

public class SinglePropertyToObjectConverter<S, T> implements ShareableConverter<S, T>
{
  private String setExpression;
  protected Setter<T,S> setter;

  public SinglePropertyToObjectConverter(){}
  
  public SinglePropertyToObjectConverter(String setExpression)
  {
    setSetExpression(setExpression);
  }
  

  @SuppressWarnings("unchecked")
  @Override
  public T convertTo(S sourceObj, T targetObj)
  {
    if( setter == null )
      setter = createSetter( (Class<S>)sourceObj.getClass(), (Class<T>)targetObj.getClass());
    setter.set(targetObj, sourceObj);
    return targetObj;
  }
  
  public Setter<T,S> createSetter( Class<S> sourceObjectClass, Class<T> targetObjectClass )
  {
    return PojoUtils.createSetter( targetObjectClass, setExpression, sourceObjectClass );
  }
  
  public void setSetExpression(String setExpression)
  {
    this.setExpression = setExpression;
  }

}
