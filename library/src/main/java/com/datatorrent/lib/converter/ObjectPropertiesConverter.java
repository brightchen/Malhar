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


/**
 * 
 * This interface provides methods to convert Object to another Object which represented by its properties
 *
 * @param <S> the type of the object
 * @param <T> the type of the object converted to
 */
@SuppressWarnings("rawtypes")
public interface ObjectPropertiesConverter< S, T >
{
  public T fromOriginObject( S obj );
  
  /**
   * This method same as <code>fromObject( Object obj )</code>. 
   * just give a chance to reuse the propertyValues object, or use sub-type of T
   * @param value
   * @param propertyValues
   * @return
   */
  public T fromOriginObject( S value, T propertyValues );
  
  public S toOriginObject( T propertyValues );
  
  public S toOriginObject( T propertyValues, S value );
  
  
}
