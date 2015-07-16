package com.datatorrent.contrib.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class TupleCacheOutputOperator<T>  extends BaseOperator
{
  private static final long serialVersionUID = 3090932382383138500L;
  private static final Logger logger = LoggerFactory.getLogger( TupleCacheOutputOperator.class );
  
  //one instance of TupleCacheOutputOperator map to one 
  private static Map< String, List<?> > receivedTuplesMap = new HashMap< String, List<?>>();
  
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>() {

    @Override
    public void process(T tuple)
    {
      processTuple( tuple );
    }
  };
  
  private String uuid;
  
  public TupleCacheOutputOperator()
  {
    uuid = java.util.UUID.randomUUID().toString();
  }
  
  public String getUuid()
  {
    return uuid;
  }

  public void processTuple( T tuple )
  {
    List<T> receivedTuples = (List<T>)receivedTuplesMap.get(uuid);
    if( receivedTuples == null )
    {
      receivedTuples = new ArrayList<T>();
      receivedTuplesMap.put(uuid, receivedTuples);
    }
    receivedTuples.add(tuple);
    logger.info( "received a tuple. total received tuple size is {}. instance is {}.", receivedTuples.size(), System.identityHashCode(this) );
  }

  public List<T> getReceivedTuples()
  {
    return (List<T>)receivedTuplesMap.get(uuid);
  }
  
  public static List<Object> getReceivedTuples( String uuid )
  {
    return (List<Object>)receivedTuplesMap.get(uuid);
  }
}