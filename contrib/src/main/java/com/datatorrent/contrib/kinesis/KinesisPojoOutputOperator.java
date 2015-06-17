package com.datatorrent.contrib.kinesis;

import java.util.List;

import com.datatorrent.common.util.Pair;
import com.datatorrent.contrib.common.FieldInfo;
import com.datatorrent.contrib.common.FieldValueGenerator;
import com.datatorrent.contrib.common.TableInfo;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

public class KinesisPojoOutputOperator extends AbstractKinesisOutputOperator<byte[], Object>
{
  private TableInfo<FieldInfo> tableInfo;

  private transient FieldValueGenerator<FieldInfo> fieldValueGenerator;

  private transient Getter<Object, String> rowGetter;
  
  @Override
  protected byte[] getRecord(byte[] value)
  {
    return value;
  }

  @Override
  protected Pair<String, byte[]> tupleToKeyValue(Object tuple)
  {
    // key
    if (rowGetter == null) 
    {
      // use string as row id
      rowGetter = PojoUtils.createGetter(tuple.getClass(), tableInfo.getRowOrIdExpression(), String.class);
    }
    String key = rowGetter.get( tuple );
    
    //value
    final List<FieldInfo> fieldsInfo = tableInfo.getFieldsInfo();
    if (fieldValueGenerator == null) {
      fieldValueGenerator = FieldValueGenerator.getFieldValueGenerator(tuple.getClass(), fieldsInfo);
    }
    return new Pair< String, byte[]>( key, fieldValueGenerator.serializeObject(tuple) );
  }


  /**
   * Table Information
   */
  public TableInfo<FieldInfo> getTableInfo()
  {
    return tableInfo;
  }

  /**
   * Table Information
   */
  public void setTableInfo(TableInfo<FieldInfo> tableInfo)
  {
    this.tableInfo = tableInfo;
  }

}
