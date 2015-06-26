/**
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.serialize;

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.contrib.util.DelegateSerializer.SerializerImplementer;
import com.datatorrent.contrib.util.ObjectPropertiesConverter.PropertyInfo;
import com.datatorrent.contrib.util.Serializer;
import com.datatorrent.contrib.util.SerializerFactory;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * 
 * The operator serialize the POJO
 * 
 * @displayName POJO Serialize
 * @category Serialize
 * @tags serialize, Pojo
 */
public class POJOSerializeOperator  extends AbstractSerializeOperator< Object >
{
  private static final long serialVersionUID = -2339881909844710454L;
  private transient Serializer serializer;
  private transient Getter<Object, String> keyGetter;
  
  private PropertyInfo[] propertyInfos;
  private String keyExpression;
  
  private SerializerImplementer serializerImplementer = SerializerImplementer.KRYO;
  
  @Override
  protected byte[] serialize(Object tuple)
  {
    return getSerializer( tuple ).serialize(tuple);
  }

  @Override
  protected String getKey(Object tuple)
  {
    if( keyGetter == null )
      keyGetter = PojoUtils.createGetter( tuple.getClass(), keyExpression, String.class );
    return keyGetter.get( tuple );
  }

  protected Serializer getSerializer( Object tuple )
  {
    if( serializer != null )
      return serializer;
    serializer = SerializerFactory.getSerializer( tuple.getClass(), serializerImplementer, propertyInfos );
    return serializer;
  }

  public String getKeyExpression()
  {
    return keyExpression;
  }

  public void setKeyExpression(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  
  public PropertyInfo[] getPropertyInfos()
  {
    return propertyInfos;
  }

  public void setPropertyInfos(PropertyInfo[] propertyInfos)
  {
    this.propertyInfos = propertyInfos;
  }

  public SerializerImplementer getSerializerImplementer()
  {
    return serializerImplementer;
  }

  public void setSerializerImplementer(SerializerImplementer serializerImplementer)
  {
    this.serializerImplementer = serializerImplementer;
  }
}
