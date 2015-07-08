package com.datatorrent.lib.util;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datatorrent.lib.converter.PropertyInfo;

/**
 * This is a copy from contrib, should be merged later.
 * 
 */
public class TestPOJO implements Serializable
{
  private static final long serialVersionUID = 2153417121590225192L;


  
  public static PropertyInfo[] getPropertyInfos()
  {
    List<PropertyInfo> propertyInfos = new ArrayList<PropertyInfo>();
    propertyInfos.add( new PropertyInfo( "rowId", "rowId", Long.class ) );
    propertyInfos.add( new PropertyInfo( "name", "name", String.class ) );
    propertyInfos.add( new PropertyInfo( "age", "age", Integer.class ) );
    propertyInfos.add( new PropertyInfo( "address", "address", String.class ) );
    
    return propertyInfos.toArray( new PropertyInfo[0]);
  }
  
  public static String getRowExpression()
  {
    return "row";
  }
  
  public static TestPOJO from( Map<String,byte[]> map )
  {
    TestPOJO testPOJO = new TestPOJO();
    for( Map.Entry<String, byte[]> entry : map.entrySet() )
    {
      //testPOJO.setValue(entry.getKey(), entry.getValue() );
    }
    return testPOJO;
  }
  
  private Long rowId = null;
  private String name;
  private int age;
  private String address;

  public TestPOJO(){}
  
  public TestPOJO(long rowId)
  {
    this(rowId, "name" + rowId, (int) rowId, "address" + rowId);
  }

  public TestPOJO(long rowId, String name, int age, String address)
  {
    this.rowId = rowId;
    setName(name);
    setAge(age);
    setAddress(address);
  }
  

  public String getRow()
  {
    return String.valueOf(rowId);
  }
  public void setRow( String row )
  {
    setRowId( Long.valueOf(row) );
  }
  public void setRowId( Long rowId )
  {
    this.rowId = rowId;
  }
  public Long getRowId()
  {
    return rowId;
  }
  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public Integer getAge()
  {
    return age;
  }

  public void setAge( Integer age)
  {
    this.age = age;
  }

  public String getAddress()
  {
    return address;
  }

  public void setAddress(String address)
  {
    this.address = address;
  }
  
  @Override
  public boolean equals( Object obj )
  {
    if( obj == null )
      return false;
    if( !( obj instanceof TestPOJO ) )
      return false;
    
    return completeEquals( (TestPOJO)obj );
  }

  public boolean outputFieldsEquals( TestPOJO other )
  {
    if( other == null )
      return false;
    if( !fieldEquals( getName(), other.getName() ) )
      return false;
    if( !fieldEquals( getAge(), other.getAge() ) )
      return false;
    if( !fieldEquals( getAddress(), other.getAddress() ) )
      return false;
    return true;
  }
  
  public boolean completeEquals( TestPOJO other )
  {
    if( other == null )
      return false;
    if( !outputFieldsEquals( other ) )
      return false;
    if( !fieldEquals( getRow(), other.getRow() ) )
      return false;
    return true;
  }
  
  public <T> boolean fieldEquals( T v1, T v2 )
  {
    if( v1 == null && v2 == null )
      return true;
    if( v1 == null || v2 == null )
      return false;
    return v1.equals( v2 );
  }
  
  @Override
  public String toString()
  {
    return String.format( "id={%d}; name={%s}; age={%d}; address={%s}", rowId, name, age, address);
  }
}