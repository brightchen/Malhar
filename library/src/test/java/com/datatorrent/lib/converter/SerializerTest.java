package com.datatorrent.lib.converter;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.codec.JavaSerializationStreamCodec;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.converter.ConverterFactory;
import com.datatorrent.lib.util.TestPOJO;
import com.datatorrent.lib.util.TupleGenerator;

public class SerializerTest
{
  private int TUPLE_SIZE = 100;
  
  @Test
  public void testSerializers()
  {
    List<StreamCodec> serializers = getSerializers();
    for( StreamCodec serializer : serializers )
    {
      for( int j=0; j<TUPLE_SIZE; ++j )
      {
        TestPOJO obj = getNextTuple();
        Assert.assertTrue( serializer.getClass().getSimpleName(), obj.equals( serializer.fromByteArray( serializer.toByteArray(obj) ) ) );
      }
    }
    
  }
  
  public List<StreamCodec> getSerializers()
  {
    List<StreamCodec> serializers = new ArrayList<StreamCodec>();
    StreamCodec kryoCodec = new KryoSerializableStreamCodec();
    StreamCodec javaCodec = new JavaSerializationStreamCodec();

//    serializers.add( ConverterFactory.getConverter( TestPOJO.class, kryoCodec, null ) );
//    serializers.add( ConverterFactory.getConverter( TestPOJO.class, javaCodec, null ) );
//
//    serializers.add( ConverterFactory.getConverter( TestPOJO.class, kryoCodec, TestPOJO.getPropertyInfos() ) );
//    serializers.add( ConverterFactory.getConverter( TestPOJO.class, javaCodec, TestPOJO.getPropertyInfos() ) );

    return serializers;
  }
  
  
  private TupleGenerator<TestPOJO> tupleGenerator;

  protected TestPOJO getNextTuple()
  {
    if( tupleGenerator == null )
      tupleGenerator = new TupleGenerator<TestPOJO>( TestPOJO.class );
    
    return tupleGenerator.getNextTuple();
  }
}
