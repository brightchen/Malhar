package com.datatorrent.contrib.kinesis;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.model.Record;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.common.FieldInfo;
import com.datatorrent.contrib.common.FieldValueGenerator;
import com.datatorrent.contrib.common.TableInfo;
import com.datatorrent.contrib.model.Employee;
import com.datatorrent.contrib.model.PojoTupleGenerateOperator;
import com.datatorrent.contrib.model.TupleGenerator;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class KinesisPojoOutputOperatorTest extends KinesisOutputOperatorTest< KinesisPojoOutputOperator, PojoTupleGenerateOperator >
{ 
  public static class EmployeeTupleGenerateOperator extends PojoTupleGenerateOperator< Employee >
  {
    public EmployeeTupleGenerateOperator()
    {
      super( Employee.class );
      setTupleNum(maxTuple);
    }
  }
  

  @Test
  public void testKinesisOutputOperatorInternal() throws Exception
  {
    KinesisPojoOutputOperator operator = new KinesisPojoOutputOperator();
    configureTestingOperator( operator );
    operator.setBatchProcessing(false);
    
    TableInfo tableInfo = new TableInfo();
    tableInfo.setFieldsInfo( Employee.getFieldsInfo() );
    tableInfo.setRowOrIdExpression( Employee.getRowExpression() );
    operator.setTableInfo( tableInfo );

    operator.setup(null);
    
    TupleGenerator generator = new TupleGenerator( Employee.class );
    
    //read tuples
    KinesisTestConsumer listener = createConsumerListener(streamName);
    String iterator = listener.prepareIterator();
    //save the tuples
    for( int i=0; i<maxTuple; ++i )
    {
      if( i%2==0)
        iterator = listener.processNextIterator(iterator);
      
      operator.processTuple( generator.getNextTuple() );
    }
    iterator = listener.processNextIterator(iterator);
  }
  
  @Override
  protected PojoTupleGenerateOperator addGenerateOperator(DAG dag)
  {
    return dag.addOperator("TestPojoGenerator", EmployeeTupleGenerateOperator.class);
  }

  @Override
  protected DefaultOutputPort getOutputPortOfGenerator(PojoTupleGenerateOperator generator)
  {
    return generator.outputPort;
  }

  @Override
  protected KinesisPojoOutputOperator addTestingOperator(DAG dag)
  {
    KinesisPojoOutputOperator operator = dag.addOperator("Test-KinesisPojoOutputOperator", KinesisPojoOutputOperator.class);
    //table info
    {
      TableInfo tableInfo = new TableInfo();
      tableInfo.setFieldsInfo( Employee.getFieldsInfo() );
      tableInfo.setRowOrIdExpression( Employee.getRowExpression() );
      operator.setTableInfo( tableInfo );
    }
    operator.setBatchProcessing(true);
    
    return operator;
  }

  /**
   * add Consumer to process the record
   */
  @Override
  protected KinesisTestConsumer createConsumerListener( String streamName )
  {
    KinesisEmployeeConsumer listener = new KinesisEmployeeConsumer(streamName);
    return listener;
  }
  
  
  public static class KinesisEmployeeConsumer extends KinesisTestConsumer
  {
    private static final Logger logger = LoggerFactory.getLogger( KinesisEmployeeConsumer.class );
    protected FieldValueGenerator<FieldInfo> fieldValueGenerator = FieldValueGenerator.getFieldValueGenerator(Employee.class, Employee.getFieldsInfo() );
    
    public KinesisEmployeeConsumer(String streamNamem )
    {
      super(streamNamem);
    }
    
    @Override
    protected void processRecord( Record record )
    {
      String partitionKey = record.getPartitionKey();
      ByteBuffer data = record.getData();
      logger.info( "partitionKey={} ", partitionKey );
      byte[] dataBytes = new byte[ data.remaining() ];
      data.get( dataBytes, 0, dataBytes.length );
      
      long key = Long.valueOf( partitionKey );
      Employee expected = new Employee( key );
      
      Employee read = (Employee)fieldValueGenerator.deserializeObject( dataBytes );
      
      if( !read.outputFieldsEquals(expected) )
      {
        logger.error( "read is not same as expected. read={}, expected={}", read, expected );
        Assert.assertTrue(false);
      }
      else
      {
        logger.info( "read is same as expected. read={}, expected={}", read, expected );
      }
    }
  }
}
