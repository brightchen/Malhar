package com.datatorrent.lib.converter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Setter;

public class NameValuePropertiesToObjectConverter<T> implements Converter<Map<String, Object>, T>, ShareableConverter<Map<String, Object>, T>
{
  protected Map<String, Setter<T, Object>> setters = new HashMap<String, Setter<T, Object>>();
  protected Class<T> targetObjectClass;

  public NameValuePropertiesToObjectConverter(){}
  
  public NameValuePropertiesToObjectConverter(Class<T> targetObjectClass)
  {
    setTargetObjectClass(targetObjectClass);
  }
  
  protected T createEmptyTargetInstance()
  {
    try {
      return targetObjectClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public T convert(Map<String, Object> sourceObj)
  {
    return convertTo(sourceObj, createEmptyTargetInstance());
  }

  public T convertTo(Map<String, Object> sourceObj, T targetObj)
  {
    for (Map.Entry<String, Setter<T, Object>> entry : setters.entrySet()) {
      entry.getValue().set(targetObj, sourceObj.get(entry.getKey()));
    }
    return targetObj;
  }

  public void setTargetObjectClass(Class<T> targetObjectClass)
  {
    this.targetObjectClass = targetObjectClass;
  }

  public void setProperties(Collection<PropertyInfo> propertyInfos)
  {
    setters.clear();
    addProperties(propertyInfos);
  }

  public void addProperties(Collection<PropertyInfo> propertyInfos)
  {
    for (PropertyInfo propertyInfo : propertyInfos) {
      addProperty(propertyInfo.name, propertyInfo.expression, propertyInfo.type);
    }
  }

  public void setProperties(PropertyInfo... propertyInfos)
  {
    setters.clear();
    addProperties(propertyInfos);
  }

  public void addProperties(PropertyInfo... propertyInfos)
  {
    for (PropertyInfo propertyInfo : propertyInfos) {
      addProperty(propertyInfo.name, propertyInfo.expression, propertyInfo.type);
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void addProperty(String name, String expression, Class type)
  {
    Setter<T, Object> getter = PojoUtils.createSetter(targetObjectClass, expression, type);
    setters.put(name, getter);
  }

}