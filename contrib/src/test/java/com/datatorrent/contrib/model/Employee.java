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

  public int getAge()
  {
    return age;
  }

  public void setAge(int age)
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

}