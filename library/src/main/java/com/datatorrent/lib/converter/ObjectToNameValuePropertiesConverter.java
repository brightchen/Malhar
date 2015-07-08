package com.datatorrent.lib.converter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

public class ObjectToNameValuePropertiesConverter<S> implements Converter<S, Map<String, Object>>, ShareableConverter<S, Map<String, Object>>
{
  protected Map<String, Getter<S, Object>> getters = new HashMap<String, Getter<S, Object>>();
  protected Class<S> sourceObjectClass;

  public ObjectToNameValuePropertiesConverter(Class<S> sourceObjectClass)
  {
    this.sourceObjectClass = sourceObjectClass;
  }

  @Override
  public Map<String, Object> convert(S sourceObj)
  {
    Map<String, Object> propertyValue = new HashMap<String, Object>();
    return convertTo(sourceObj, propertyValue);
  }

  public Map<String, Object> convertTo(S sourceObj, Map<String, Object> propertyValues)
  {
    propertyValues.clear();
    for (Map.Entry<String, Getter<S, Object>> entry : getters.entrySet()) {
      propertyValues.put(entry.getKey(), entry.getValue().get(sourceObj));
    }
    return propertyValues;
  }

  public void setProperties(Collection<PropertyInfo> propertyInfos)
  {
    getters.clear();
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
    getters.clear();
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
    Getter<S, Object> getter = PojoUtils.createGetter(sourceObjectClass, expression, type);
    getters.put(name, getter);
  }

}
