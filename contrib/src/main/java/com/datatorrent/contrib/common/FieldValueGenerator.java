package com.datatorrent.contrib.common;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.Setter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * 
 * it's better to provide another parameter C for type of Object, so de/serializeObject() can be type-safe
 *
 * @param <T>
 */
public class FieldValueGenerator< T extends FieldInfo >
{
  private static final Logger logger = LoggerFactory.getLogger( FieldValueGenerator.class );
  private Class<?> clazz;
  private List<T> fieldInfos;
	private Map<T, Getter<Object,Object>> _fieldGetterMap = null;
	private Map<T, Setter<Object,Object>> _fieldSetterMap = null;
	
	//it's better to same kryo instance for both de/serialize
	private Kryo _kryo = null;
  
	private FieldValueGenerator(){}
	
	@SuppressWarnings("unchecked")
  public static < T extends FieldInfo > FieldValueGenerator<T> getFieldValueGenerator(final Class<?> clazz, List<T> fieldInfos)
  {
    FieldValueGenerator<T> fieldValueGenerator = new FieldValueGenerator<T>();
    fieldValueGenerator.clazz = clazz;
    fieldValueGenerator.fieldInfos = fieldInfos;
    return fieldValueGenerator;
  }
	
	protected Map<T, Getter<Object,Object>> getFieldGetterMap()
	{
    if( fieldInfos != null && _fieldGetterMap == null )
    {
      synchronized( this )
      {
        if( _fieldGetterMap == null )
        {
          _fieldGetterMap = new HashMap<T,Getter<Object,Object>>();
          for( T fieldInfo : fieldInfos )
          {
            Getter<Object,Object> getter = PojoUtils.createGetter(clazz, fieldInfo.getColumnExpression(), fieldInfo.getType().getJavaType() );
            _fieldGetterMap.put( fieldInfo, getter );
          }
        }
      }
    }
    return _fieldGetterMap;
	}
	
	protected Map<T, Setter<Object,Object>> getFieldSetterMap()
	{
    if( fieldInfos != null && _fieldSetterMap == null )
    {
      synchronized( this )
      {
        if( _fieldSetterMap == null )
        {
          _fieldSetterMap = new HashMap<T,Setter<Object,Object>>();
          for( T fieldInfo : fieldInfos )
          {
            Setter<Object,Object> setter = PojoUtils.createSetter(clazz, fieldInfo.getColumnExpression(), fieldInfo.getType().getJavaType() );
            _fieldSetterMap.put( fieldInfo, setter );
          }
        }
      }
    }
    return _fieldSetterMap;
	}
	 
	
	/**
	 * 
	 * @param obj
	 * @return a map from FieldInfo to columnValue
	 */
	public Map< T, Object > getFieldsValue( Object obj )
	{
		Map< T, Object > fieldsValue = new HashMap< T, Object>();
		for( Map.Entry< T, Getter<Object,Object>> entry : getFieldGetterMap().entrySet() )
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
	public byte[] serializeObject( Object obj )
	{
	//if don't have field information, just convert the whole object to byte[]
	  Object convertObj = obj;
	  
	  //if fields are specified, convert to map and then convert map to byte[]
	  Map<T, Getter<Object,Object>> getterMap = getFieldGetterMap();
	  if( getterMap != null && !getterMap.isEmpty() )
	  {
  	  Map< String, Object > fieldsValue = new HashMap< String, Object>();
      for( Map.Entry< T, Getter<Object,Object>> entry : getterMap.entrySet() )
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
    

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);

    getKryo().writeClassAndObject(output, convertObj);
    output.flush();
    //output.toBytes() is empty.
    return os.toByteArray();
	}

	
	public Object deserializeObject( byte[] bytes )
	{
    Object obj = getKryo().readClassAndObject( new Input( bytes ) );
    {
      Map<T, Getter<Object,Object>> getterMap = getFieldGetterMap();
      if( getterMap == null || getterMap.isEmpty() )
        return obj;
    }
    
    // the obj in fact is a map, convert from map to object
    try
    {
      Map valueMap = (Map)obj;
      obj = clazz.newInstance();
      Map<T, Setter<Object,Object>> setterMap = getFieldSetterMap();
      for( Map.Entry< T, Setter<Object,Object>> entry : setterMap.entrySet() )
      {
        T fieldInfo = entry.getKey();
        Setter<Object,Object> setter = entry.getValue();
        if( setter != null )
        {
          setter.set(obj, valueMap.get( fieldInfo.getColumnName() ) );
        }
      }
      return obj;
    }
    catch( Exception e )
    {
      logger.warn( "Coverting map to obj exception. ", e );
      return obj;
    }
	}
	
	protected Kryo getKryo()
	{
	  if( _kryo == null )
	  {
	    synchronized( this )
	    {
	      if( _kryo == null )
	        _kryo = new Kryo();
	    }
	  }
	  return _kryo;
	}
}
