package com.datatorrent.contrib.model;

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.contrib.common.FieldInfo;
import com.datatorrent.contrib.common.FieldInfo.SupportType;

public class Employee
{
  public static List<FieldInfo> getFieldsInfo()
  {
    List<FieldInfo> fieldsInfo = new ArrayList<FieldInfo>();
    fieldsInfo.add( new FieldInfo( "name", "name", SupportType.STRING ) );
    fieldsInfo.add( new FieldInfo( "age", "age", SupportType.INTEGER ) );
    fieldsInfo.add( new FieldInfo( "address", "address", SupportType.STRING ) );
    
    return fieldsInfo;
  }
  
  public static String getRowExpression()
  {
    return "row";
  }
  
  
  private long rowId = 0;
  private String name;
  private int age;
  private String address;

  public Employee(){}
  
  public Employee(long rowId)
  {
    this(rowId, "name" + rowId, (int) rowId, "address" + rowId);
  }

  public Employee(long rowId, String name, int age, String address)
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

  public boolean outputFieldsEquals( Employee other )
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
  
  public boolean completeEquals( Employee other )
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
}