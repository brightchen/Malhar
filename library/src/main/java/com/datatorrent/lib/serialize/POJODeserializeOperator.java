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
package com.datatorrent.lib.serialize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.serialize.ObjectPropertiesConverter.PropertyInfo;
import com.datatorrent.netlet.util.Slice;

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

  private transient StreamCodec serializer;
  private transient Slice slice = new Slice( new byte[1] );
  
  private Class tupleType;
  private PropertyInfo[] propertyInfos;
  // the type of tuple
  private String tupleTypeName;
  
  private StreamCodec<Object> codecImplementer = new KryoSerializableStreamCodec<Object>();
  
  
  @Override
  protected Object deserialize(byte[] value)
  {
    return getSerializer().fromByteArray( getSlice(value) );
  }
  

  protected StreamCodec getSerializer()
  {
    if( serializer != null )
      return serializer;
    serializer = SerializerFactory.getSerializer( tupleType, codecImplementer, propertyInfos );
    return serializer;
  }
  

  protected Slice getSlice( byte[] value )
  {
    slice.buffer = value;
    slice.offset = 0;
    slice.length = value.length;
    return slice;
  }
  
  public PropertyInfo[] getPropertyInfos()
  {
    return propertyInfos;
  }

  public void setPropertyInfos(PropertyInfo[] propertyInfos)
  {
    this.propertyInfos = propertyInfos;
  }

  public StreamCodec<Object> getCodecImplementer()
  {
    return codecImplementer;
  }

  public void setCodecImplementer(StreamCodec<Object> codecImplementer)
  {
    this.codecImplementer = codecImplementer;
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
