package com.datatorrent.contrib.model;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;

/**
 * Mark this class as abstract just because it doesn't provide default constructor and can't be used directly as an Operator
 * 
 * @param <T>
 */
public abstract class PojoTupleGenerateOperator<T> implements InputOperator, ActivationListener<OperatorContext>
{
  protected final int DEFAULT_TUPLE_NUM = 10000;
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();
  
  private TupleGenerator<T> tupleGenerator = null;
  private Class<T> tupleClass;

  public PojoTupleGenerateOperator( Class<T> tupleClass )
  {
    this.tupleClass = tupleClass;
  }
  
  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(OperatorContext ctx)
  {
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public void emitTuples()
  {
    for (int i = 0; i<getTupleNum(); ++i ) 
    {
      outputPort.emit (getNextTuple() );
    }
  }
  
  protected int getTupleNum()
  {
    return DEFAULT_TUPLE_NUM;
  }

  protected T getNextTuple()
  {
    if( tupleGenerator == null )
      tupleGenerator = createTupleGenerator();

    return tupleGenerator.getNextTuple();
  }

  protected TupleGenerator<T> createTupleGenerator()
  {
    return new TupleGenerator( tupleClass );
  }
}