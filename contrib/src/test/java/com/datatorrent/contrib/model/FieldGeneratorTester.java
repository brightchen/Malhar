package com.datatorrent.contrib.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.contrib.common.FieldInfo;
import com.datatorrent.contrib.common.FieldValueGenerator;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class FieldGeneratorTester
{
  private static final Logger logger = LoggerFactory.getLogger( FieldGeneratorTester.class );
      
  @Test
  public void testGetSerializedObject()
  {
    for( int i = 0; i<10; ++i )
    {
      Assert.assertTrue( testGetSerializedObject( new Employee(i) ) );
    }
  }
  
  public boolean testGetSerializedObject( Employee employee )
  {
    FieldValueGenerator<FieldInfo> generator = FieldValueGenerator.getFieldValueGenerator( Employee.class, Employee.getFieldsInfo() );

    byte[] value = generator.serializeObject( employee );
    
    logger.info( "value = {}, length = {}", value, value.length );
    

    Employee expected = employee;
    
    Employee read = (Employee)generator.deserializeObject( value );

    if( !read.outputFieldsEquals(expected) )
    {
      logger.error( "read is not same as expected." );
      return false;
    }
    else
    {
      logger.error( "read is same as expected." );
      return true;
    }
  }
}
