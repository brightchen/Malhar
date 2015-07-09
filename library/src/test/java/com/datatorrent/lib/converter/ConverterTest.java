package com.datatorrent.lib.converter;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.Pair;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.util.TestPOJO;
import com.datatorrent.lib.util.TupleGenerator;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ConverterTest
{
  private static final Logger logger = LoggerFactory.getLogger(ConverterTest.class);
      
  private int TUPLE_SIZE = 100;

  private List<Pair<Converter, Converter>> converterPairs = new ArrayList<Pair<Converter, Converter>>();
  
  
  @Before
  public void setup()
  {
    StreamCodec codec = new KryoSerializableStreamCodec();
    converterPairs.add(new Pair(new DefaultKeyValueOutputConverter(TestPOJO.getRowExpression(), codec), 
        new DefaultKeyValueInputConverter(TestPOJO.getRowExpression(), codec) ));
    converterPairs.add(new Pair(new DefaultObjectPropertiesOutputConverter(TestPOJO.getRowExpression(), codec, TestPOJO.getPropertyInfos()), 
        new DefaultObjectPropertiesInputConverter(TestPOJO.getRowExpression(), TestPOJO.class, codec, TestPOJO.getPropertyInfos()) ));
  }
  
  protected <S, T> void addConverterPair(Converter<S, T> converter, Converter<T, S> recoverConverter)
  {
    converterPairs.add(new Pair(converter, recoverConverter) );
  }
  
  @Test
  public void testConverters()
  {
    for(Pair<Converter, Converter> convertPair : converterPairs)
    {
      testConverterPair(convertPair.first,convertPair.second);
    }
    logger.info("Test Done.");
  }
  
  public void testConverterPair(Converter converter1, Converter converter2)
  {
    for( int j=0; j<TUPLE_SIZE; ++j )
    {
      TestPOJO obj = getNextTuple();
      Object tmp = converter1.convert(obj);
      Assert.assertFalse( converter1.getClass().getSimpleName(), obj.equals(tmp) );
      Object result = converter2.convert(tmp);
      Assert.assertTrue( String.format( "converter1=%s, converter2=%s", converter1.getClass().getSimpleName(), converter2.getClass().getSimpleName() ), obj.equals(result) );
    }
    logger.info("Test Done for converter: {}, {}", converter1.getClass().getSimpleName(), converter2.getClass().getSimpleName());
  }
  
  
  
  private TupleGenerator<TestPOJO> tupleGenerator;

  protected TestPOJO getNextTuple()
  {
    if( tupleGenerator == null )
      tupleGenerator = new TupleGenerator<TestPOJO>( TestPOJO.class );
    
    return tupleGenerator.getNextTuple();
  }
}
