package com.datatorrent.lib.converter;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Setter;

public class SinglePropertyToObjectConverter<S, T> implements ShareableConverter<S, T>
{
  private String setExpression;
  protected Setter<T,S> setter;

  public SinglePropertyToObjectConverter(){}
  
  public SinglePropertyToObjectConverter(String setExpression)
  {
    setSetExpression(setExpression);
  }
  

  @SuppressWarnings("unchecked")
  @Override
  public T convertTo(S sourceObj, T targetObj)
  {
    if( setter == null )
      setter = createSetter( (Class<S>)sourceObj.getClass(), (Class<T>)targetObj.getClass());
    setter.set(targetObj, sourceObj);
    return targetObj;
  }
  
  public Setter<T,S> createSetter( Class<S> sourceObjectClass, Class<T> targetObjectClass )
  {
    return PojoUtils.createSetter( targetObjectClass, setExpression, sourceObjectClass );
  }
  
  public void setSetExpression(String setExpression)
  {
    this.setExpression = setExpression;
  }

}
