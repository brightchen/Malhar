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


import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/**
 * 
 * The operator convert the tuples
 * the sub-class can override <code>createConverter</code> or <code>createConverter( Class<S> tupleClass )</code> 
 * to set the converter. And the client can use <code>setConverter</code> to set the converter
 * 
 * @displayName Abstract Converter
 * @category Converter
 * @tags converter
 */
public class BaseConverterOperator<S, T> extends BaseOperator
{
  /**
   * Output port that emits tuples into the DAG.
   */
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();
  
  protected Converter<S, T> converter;
  /**
   * The input port on which tuples are received
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<S> inputPort = new DefaultInputPort<S>() {
    @Override
    public void process(S t)
    {
      processTuple(t);
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    if( converter == null )
      createConverter();
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
  @SuppressWarnings("unchecked")
  public void processTuple(S tuple)
  {
    if( converter == null )
      createConverter( (Class<S>)tuple.getClass() );
    outputPort.emit( converter.convert( tuple ) );
  }
  
  protected void createConverter(){}
  
  protected void createConverter( Class<S> tupleClass ){}

  public Converter<S, T> getConverter()
  {
    return converter;
  }

  
}
