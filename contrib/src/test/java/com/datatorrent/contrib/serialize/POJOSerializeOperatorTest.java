package com.datatorrent.contrib.serialize;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.util.TestPOJO;
import com.datatorrent.contrib.util.TupleCacheOutputOperator;
import com.datatorrent.contrib.util.TupleGenerateCacheOperator;

public class POJOSerializeOperatorTest
{
  public static enum OPERATOR
  {
    GENERATOR,
    SERIALIZER,
    DESERIALIZER,
    OUTPUT
  };
  
  private static final Logger logger = LoggerFactory.getLogger( POJOSerializeOperatorTest.class );
  
  private static final int TUPLE_NUM = 10;
  
  public static class MyGenerator extends TupleGenerateCacheOperator<TestPOJO>
  {
    public MyGenerator()
    {
      this.setTupleType( TestPOJO.class );
    }
  }
  
  @Test
  public void test() throws Exception
  {

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    DAG dag = lma.getDAG();

    // Create ActiveMQStringSinglePortOutputOperator
    MyGenerator generator = dag.addOperator( OPERATOR.GENERATOR.name(), MyGenerator.class);
    generator.setTupleNum( TUPLE_NUM );
    
    POJOSerializeOperator serializeOperator = dag.addOperator("SerializeOperator", POJOSerializeOperator.class);
    serializeOperator.setKeyExpression( TestPOJO.getRowExpression() );
    serializeOperator.setPropertyInfos( TestPOJO.getPropertyInfos() );
    
    POJODeserializeOperator deserializeOperator = dag.addOperator("DeserializeOperator", POJODeserializeOperator.class);
    deserializeOperator.setTupleType(TestPOJO.class);
    deserializeOperator.setPropertyInfos( TestPOJO.getPropertyInfos() );
    
    TupleCacheOutputOperator output = dag.addOperator(OPERATOR.OUTPUT.name(), TupleCacheOutputOperator.class);
    
    // Connect ports
    dag.addStream("queue1", generator.outputPort, serializeOperator.inputPort ).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("queue2", serializeOperator.outputPort, deserializeOperator.pairInputPort ).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("queue3", deserializeOperator.outputPort, output.inputPort ).setLocality(Locality.CONTAINER_LOCAL);
    
    
    Configuration conf = new Configuration(false);
    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    //generator.doneLatch.await();
    while(true)
    {
      try
      {
        Thread.sleep(1000);
      }
      catch( Exception e ){}
      
      logger.info( "Received tuple number {}, instance is {}.", output.getReceivedTuples() == null ? 0 : output.getReceivedTuples().size(), System.identityHashCode( output ) );
      if( output.getReceivedTuples() != null && output.getReceivedTuples().size() == TUPLE_NUM )
        break;
    }
    
    lc.shutdown();

    
    validate( generator.getTuples(), output.getReceivedTuples() );

  }
  
  protected void validate( List<TestPOJO> generatedTuples, List<TestPOJO> receivedTuples )
  {
    Assert.assertEquals( "size not same.", generatedTuples.size(), receivedTuples.size() );
    for( int i=0; i<generatedTuples.size(); ++i )
    {
      Assert.assertTrue( "Not equal.", generatedTuples.get(i).completeEquals(receivedTuples.get(i)) );
    }
  }
}
