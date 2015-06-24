package com.datatorrent.contrib.serialize;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.contrib.util.ObjectPropertiesConverter.PropertyInfo;
import com.datatorrent.contrib.util.Serializer;
import com.datatorrent.contrib.util.SerializerFactory;
import com.datatorrent.contrib.util.TestPOJO;
import com.datatorrent.contrib.util.TupleGenerator;
import com.datatorrent.contrib.util.DelegateSerializer.SerializerImplementer;

public class SerializerTest
{
  private int TUPLE_SIZE = 100;
  
  @Test
  public void testSerializers()
  {
    List<Serializer> serializers = getSerializers();
    for( Serializer serializer : serializers )
    {
      for( int j=0; j<TUPLE_SIZE; ++j )
      {
        TestPOJO obj = getNextTuple();
        Assert.assertTrue( serializer.getClass().getSimpleName(), obj.equals( serializer.deserialize( serializer.serialize(obj) ) ) );
      }
    }
    
  }
  
  public List<Serializer> getSerializers()
  {
    List<Serializer> serializers = new ArrayList<Serializer>();
    serializers.add( SerializerFactory.getSerializer( TestPOJO.class, SerializerImplementer.KRYO, null ) );
    serializers.add( SerializerFactory.getSerializer( TestPOJO.class, SerializerImplementer.JAVA, null ) );

    serializers.add( SerializerFactory.getSerializer( TestPOJO.class, SerializerImplementer.KRYO, TestPOJO.getPropertyInfos() ) );
    serializers.add( SerializerFactory.getSerializer( TestPOJO.class, SerializerImplementer.JAVA, TestPOJO.getPropertyInfos() ) );

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
