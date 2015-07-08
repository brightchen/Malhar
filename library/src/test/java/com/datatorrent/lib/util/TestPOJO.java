/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.util;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datatorrent.lib.converter.PropertyInfo;
import com.datatorrent.lib.util.FieldInfo.SupportType;


public class TestPOJO implements Serializable
{
  private static final long serialVersionUID = 2153417121590225192L;
  private static final String UTF8_ENCODING = "UTF-8";
  private static final Charset UTF8_CHARSET = Charset.forName(UTF8_ENCODING);
  public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

  public static List<FieldInfo> getFieldsInfo()
  {
    List<FieldInfo> fieldsInfo = new ArrayList<FieldInfo>();
    fieldsInfo.add( new FieldInfo( "name", "name", SupportType.STRING ) );
    fieldsInfo.add( new FieldInfo( "age", "age", SupportType.INTEGER ) );
    fieldsInfo.add( new FieldInfo( "address", "address", SupportType.STRING ) );
    
    return fieldsInfo;
  }
  
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
      testPOJO.setValue(entry.getKey(), entry.getValue() );
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
  
  public void setValue( String fieldName, byte[] value )
  {
    if( "row".equalsIgnoreCase(fieldName) )
    {
      setRow( new String(value, UTF8_CHARSET) );
      return;
    }
    if( "name".equalsIgnoreCase(fieldName))
    {
      setName( new String(value, UTF8_CHARSET) );
      return;
    }
    if( "address".equalsIgnoreCase(fieldName))
    {
      setAddress( new String(value, UTF8_CHARSET) );
      return;
    }
    if( "age".equalsIgnoreCase(fieldName))
    {
      setAge( toInt(value) );
      return;
    }
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
  
  public static int toInt(byte[] bytes)
  {
    int n = 0;
    for (int i = 0; i < bytes.length; i++) {
      n <<= 8;
      n ^= bytes[i] & 0xFF;
    }
    return n;
  }
}