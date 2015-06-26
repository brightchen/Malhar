package com.datatorrent.contrib.util;

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;

public class TupleCacheOutputOperator<T>  extends BaseOperator
{
  private static final long serialVersionUID = 3090932382383138500L;

  private List<T> receivedTuples = new ArrayList<T>();
  
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>() {

    @Override
    public void process(T tuple)
    {
      processTuple( tuple );
    }

  };
      
  public TupleCacheOutputOperator(){}

  public void processTuple( T tuple )
  {
    receivedTuples.add(tuple);
  }

  public List<T> getReceivedTuples()
  {
    return receivedTuples;
  }
}
