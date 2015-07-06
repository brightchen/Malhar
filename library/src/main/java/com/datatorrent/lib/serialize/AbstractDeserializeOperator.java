package com.datatorrent.lib.serialize;


import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.Pair;

/**
 * 
 * The operator serialize the POJO
 * 
 * @displayName Abstract Serialize
 * @category Serialize
 * @tags serialize
 */
public abstract class AbstractDeserializeOperator<T> extends BaseOperator
{
  private static final long serialVersionUID = 4543434659573846534L;

  /**
   * Output port that emits tuples into the DAG.
   */
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();
  
  /**
   * The pairInput expect tuple is a pair of String and byte[]
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<Pair<String,byte[]>> pairInputPort = new DefaultInputPort<Pair<String,byte[]>>() {
    @Override
    public void process(Pair<String,byte[]> t)
    {
      processPairTuple(t);
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<byte[]> rawInputPort = new DefaultInputPort<byte[]>() {
    @Override
    public void process( byte[] t )
    {
      processRawTuple(t);
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
  public void processPairTuple(Pair<String,byte[]> tuple)
  {
    outputPort.emit( deserialize( tuple.second ) );
  }
  
  public void processRawTuple( byte[] tuple)
  {
    outputPort.emit( deserialize( tuple ) );
  }
  
  protected abstract T deserialize( byte[] value );
}