package com.datatorrent.lib.converter;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.Pair;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * 
 * The input of this Operator is POJO, and the output is the key-value
 * 
 */
public abstract class AbstractPOJOKeyValueConverterOperator< O, K > extends BaseConverterOperator< Object, O >
{
  public static interface KeyValueSetter< O, K >
  {
    public void setKey( O outputTuple, K key );
    public void setValue( O outputTuple, byte[] value );
  }

  
  private String keyExpression;
  private String keyTypeName;
  private KeyValueSetter<O,K> keyValueSetter;
  private boolean reuseOutputTuple = false;
  private Class<O> outputTupleType;
  
  private transient Getter<Object, K> keyGetter;
  private transient Class<K> keyType;
  private transient O outputTuple;      //internal use
  
  
  @SuppressWarnings("unchecked")
  @Override
  public void setup(OperatorContext context)
  {
    if( keyType == null )
    {
      try {
        keyType = (Class<K>)Class.forName(keyTypeName);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException( e.getMessage() );
      }
    }
  }
  
  //the emitted tuple should not be reuseable in container-local mode.
  @Override
  public void processTuple(Object tuple)
  {
    try
    {
      if( !reuseOutputTuple || outputTuple == null )
        outputTuple = outputTupleType.newInstance();
      
      keyValueSetter.setKey( outputTuple, getKey( tuple ) );
      keyValueSetter.setValue(outputTuple, serializeValue( tuple ));
  
      outputPort.emit( outputTuple );
    }
    catch( Exception e )
    {
      throw new RuntimeException( e.getMessage() );
    }
  }

  
  protected K getKey(Object tuple)
  {
    if( keyGetter == null )
      keyGetter = PojoUtils.createGetter( tuple.getClass(), keyExpression, keyType );
    return keyGetter.get( tuple );
  }
  
  protected abstract byte[] serializeValue( Object tuple );

  
  public String getKeyExpression()
  {
    return keyExpression;
  }

  public void setKeyExpression(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  public boolean isReuseOutputTuple()
  {
    return reuseOutputTuple;
  }

  public void setReuseOutputTuple(boolean reuseOutputTuple)
  {
    this.reuseOutputTuple = reuseOutputTuple;
  }

  public Class<O> getOutputTupleType()
  {
    return outputTupleType;
  }

  public void setOutputTupleType(Class<O> outputTupleType)
  {
    this.outputTupleType = outputTupleType;
  }


  
}
