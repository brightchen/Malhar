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

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

public class ObjectPropertiesHolder<S>
{
  //use list to make sure the property and getter are one to one match
  protected List<PropertyInfo> propertyInfos = new ArrayList<PropertyInfo>();
  protected List< Getter<S, Object> > getters = new ArrayList< Getter<S,Object> >();

  public ObjectPropertiesHolder(PropertyInfo ... properties)
  {
    setProperties(properties);
  }
  

  @SuppressWarnings("unchecked")
  protected void generateGetters(Class<S> sourceObjectClass)
  {
    if(getters==null)
      getters = new ArrayList< Getter<S,Object> >();
    else
      getters.clear();
    
    for(PropertyInfo propertyInfo : propertyInfos)
    {
      Getter<S,Object> getter = PojoUtils.createGetter(sourceObjectClass, propertyInfo.getExpression(), propertyInfo.getType());
      getters.add(getter);
    }
  }
  
  
  public void setProperties( PropertyInfo ... properties )
  {
    addProperties( properties );
  }

  public void addProperties( PropertyInfo ... properties )
  {
    for( PropertyInfo element : properties )
    {
      addProperty(element);
    }
  }

  public void setProperties( List<PropertyInfo> properties )
  {
    propertyInfos.clear();
    propertyInfos.addAll(properties);
  }
  
  public void addProperty( PropertyInfo property )
  {
    propertyInfos.add(property);
  }
  
}
