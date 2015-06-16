package com.datatorrent.contrib.kinesis;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.common.TableInfo;
import com.datatorrent.contrib.model.Employee;
import com.datatorrent.contrib.model.PojoTupleGenerateOperator;

public class KinesisPojoOutputOperatorTest extends KinesisOutputOperatorTest< KinesisPojoOutputOperator, PojoTupleGenerateOperator >
{
  public static class EmployeeTupleGenerateOperator extends PojoTupleGenerateOperator< Employee >
  {
    public EmployeeTupleGenerateOperator()
    {
      super( Employee.class );
    }
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
    
    return operator;
  }

}
