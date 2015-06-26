package com.datatorrent.contrib.util;

import java.util.ArrayList;
import java.util.List;

public class TupleGenerateCacheOperator<T> extends POJOTupleGenerateOperator<T>
{
  private List<T> tuples = new ArrayList<T>();
  
  protected void tupleEmitted( T tuple )
  {
    tuples.add(tuple);
  }
  
  public List<T> getTuples()
  {
    return tuples;
  }
}
