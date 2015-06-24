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

import java.io.IOException;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.contrib.util.Serializer;

/**
 * 
 * The operator serialize the POJO
 * 
 * @displayName Abstract Serialize
 * @category Serialize
 * @tags serialize
 */
public abstract class AbstractSerializeOperator<T> extends BaseOperator
{
  private Serializer serializer;
  
  /**
   * The input port on which tuples are received for writing.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>() {
    @Override
    public void process(T t)
    {
      processTuple(t);
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void teardown()
  {
  }

  /**
   * Processes the incoming tuple
   *
   * @param tuple
   *          a tuple.
   */
  public abstract void processTuple(T tuple);
}
