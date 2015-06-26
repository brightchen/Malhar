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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.contrib.util.DelegateSerializer.SerializerImplementer;
import com.datatorrent.contrib.util.ObjectPropertiesConverter.PropertyInfo;
import com.datatorrent.contrib.util.Serializer;
import com.datatorrent.contrib.util.SerializerFactory;

/**
 * 
 * The operator deserialize the POJO
 * 
 * @displayName POJO Serialize
 * @category Serialize
 * @tags deserialize, Pojo
 */
@SuppressWarnings("rawtypes")
public class POJODeserializeOperator  extends AbstractDeserializeOperator< Object >
{
  private static final long serialVersionUID = 4777531703441029330L;
  private static final Logger logger = LoggerFactory.getLogger( POJODeserializeOperator.class );

  private transient Serializer serializer;
  
  private Class tupleType;
  private PropertyInfo[] propertyInfos;
  // the type of tuple
  private String tupleTypeName;
  
  private SerializerImplementer serializerImplementer = SerializerImplementer.KRYO;
  
  @Override
  protected Object deserialize(byte[] value)
  {
    return getSerializer().deserialize(value);
  }
  

  protected Serializer getSerializer()
  {
    if( serializer != null )
      return serializer;
    serializer = SerializerFactory.getSerializer( tupleType, serializerImplementer, propertyInfos );
    return serializer;
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


  public String getTupleTypeName()
  {
    return tupleType.getName();
  }

  public void setTupleTypeName(String tupleTypeName) throws ClassNotFoundException
  {
    this.tupleTypeName = tupleTypeName;
    this.tupleType = Class.forName(tupleTypeName);
  }
  
  public void setTupleType( Class tupleType )
  {
    this.tupleType = tupleType;
    this.tupleTypeName = tupleType.getName();
  }

}
