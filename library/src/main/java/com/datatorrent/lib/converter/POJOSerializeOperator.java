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
package com.datatorrent.lib.converter;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.Pair;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * 
 * The operator serialize the POJO
 * 
 * @displayName POJO Serialize
 * @category Serialize
 * @tags serialize, Pojo
 * 
 * This class implement how to serialize the value
 */
public class POJOSerializeOperator extends AbstractPOJOKeyValueConverterOperator< Pair<String, byte[] >, String >
{
  private static final long serialVersionUID = -2339881909844710454L;
  
  private transient StreamCodec serializer;
//  private transient Slice slice = new Slice( new byte[1] );
  
  private transient Getter<Object, String> keyGetter;
  
  private PropertyInfo[] propertyInfos;
  private StreamCodec<Object> codecImplementer = new KryoSerializableStreamCodec<Object>();
  
  
  protected StreamCodec getSerializer( Object tuple )
  {
    if( serializer != null )
      return serializer;
    //serializer = ConverterFactory.getConverter( tuple.getClass(), codecImplementer, propertyInfos );
    return serializer;
  }
  
  @Override
  protected byte[] serializeValue(Object tuple)
  {
    //NOTES: here assume the Slice returned by serializer is only a wrapper of one byte array.
    return getSerializer( tuple ).toByteArray(tuple).buffer;
  }

//  protected Slice getSlice( byte[] value )
//  {
//    slice.buffer = value;
//    slice.offset = 0;
//    slice.length = value.length;
//    return slice;
//  }
  
  
  public PropertyInfo[] getPropertyInfos()
  {
    return propertyInfos;
  }

  public void setPropertyInfos(PropertyInfo[] propertyInfos)
  {
    this.propertyInfos = propertyInfos;
  }



}
