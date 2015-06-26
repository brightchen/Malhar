package com.datatorrent.contrib.serialize;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.util.TestPOJO;
import com.datatorrent.contrib.util.TupleCacheOutputOperator;
import com.datatorrent.contrib.util.TupleGenerateCacheOperator;

public class POJOSerializeOperatorTest
{
  
  public static class MyGenerator extends TupleGenerateCacheOperator<TestPOJO>
  {
    //private CountDownLatch doneLatch = new CountDownLatch(1);
    private boolean tupleEmitDone = false;
    public MyGenerator()
    {
      this.setTupleType( TestPOJO.class );
    }
    
    @Override
    protected void tupleEmitDone()
    {
      //doneLatch.countDown();
      tupleEmitDone = true;
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
    MyGenerator generator = dag.addOperator("Generator", MyGenerator.class);
    
    POJOSerializeOperator serializeOperator = dag.addOperator("SerializeOperator", POJOSerializeOperator.class);
    serializeOperator.setKeyExpression( TestPOJO.getRowExpression() );
    serializeOperator.setPropertyInfos( TestPOJO.getPropertyInfos() );
    
    POJODeserializeOperator deserializeOperator = dag.addOperator("DeserializeOperator", POJODeserializeOperator.class);
    deserializeOperator.setTupleType(TestPOJO.class);
    deserializeOperator.setPropertyInfos( TestPOJO.getPropertyInfos() );
    
    TupleCacheOutputOperator output = dag.addOperator("OutputOperator", TupleCacheOutputOperator.class);
    
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
        if( generator.tupleEmitDone )
          break;
      }
      catch( Exception e ){}
    }
    
    validate( generator.getTuples(), output.getReceivedTuples() );
    
    lc.shutdown();
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
