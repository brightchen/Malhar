package com.datatorrent.contrib.common;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class FieldValueGenerator< T extends FieldInfo >
{
	private Map<T, Getter<Object,Object>> fieldGetterMap = new HashMap<T,Getter<Object,Object>>();
	
	private FieldValueGenerator(){}
	
	@SuppressWarnings("unchecked")
  public static < T extends FieldInfo > FieldValueGenerator<T> getFieldValueGenerator(final Class<?> clazz, List<T> fieldInfos)
  {
    FieldValueGenerator<T> fieldValueGenerator = new FieldValueGenerator<T>();
    if( fieldInfos != null )
    {
      for( T fieldInfo : fieldInfos )
      {
      	Getter<Object,Object> getter = PojoUtils.createGetter(clazz, fieldInfo.getColumnExpression(), fieldInfo.getType().getJavaType() );
      	fieldValueGenerator.fieldGetterMap.put( fieldInfo, getter );
      }
    }
    return fieldValueGenerator;
  }
	
	/**
	 * 
	 * @param obj
	 * @return a map from FieldInfo to columnValue
	 */
	public Map< T, Object > getFieldsValue( Object obj )
	{
		Map< T, Object > fieldsValue = new HashMap< T, Object>();
		for( Map.Entry< T, Getter<Object,Object>> entry : fieldGetterMap.entrySet() )
		{
			Getter<Object,Object> getter = entry.getValue();
			if( getter != null )
			{
				Object value = getter.get(obj);
				fieldsValue.put(entry.getKey(), value);
			}
		}
		return fieldsValue;
	}
	
	/**
	 * get the object which is serialized.
	 * this method will convert the object into a map from column name to column value and then serialize it
	 * 
	 * @param obj
	 * @return
	 */
	public byte[] getSerializedObject( Object obj )
	{
	//if don't have field information, just convert the whole object to byte[]
	  Object convertObj = obj;
	  
	  //if fields are specified, convert to map and then convert map to byte[]
	  if( fieldGetterMap != null && !fieldGetterMap.isEmpty() )
	  {
  	  Map< String, Object > fieldsValue = new HashMap< String, Object>();
      for( Map.Entry< T, Getter<Object,Object>> entry : fieldGetterMap.entrySet() )
      {
        Getter<Object,Object> getter = entry.getValue();
        if( getter != null )
        {
          Object value = getter.get(obj);
          fieldsValue.put(entry.getKey().getColumnName(), value);
        }
      }
      convertObj = fieldsValue;
	  }
    
    Kryo kryo = new Kryo();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);

    kryo.writeClassAndObject(output, convertObj);
    output.flush();
    return output.toBytes();

	}
	
}
