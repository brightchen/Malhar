package com.datatorrent.lib.converter;

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.common.util.Pair;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

@SuppressWarnings("rawtypes")
public class ObjectToSequentialPropertiesConverter<S> implements Converter<S, List>, ShareableConverter<S, List>
{
  protected List< Getter<S,Object> > getters = new ArrayList< Getter<S,Object> >();
  protected Class<S> sourceObjectClass;

  public ObjectToSequentialPropertiesConverter( Class<S> sourceObjectClass )
  {
    this.sourceObjectClass = sourceObjectClass;
  }

  @Override
  public List convert(S sourceObj)
  {
    List< Object > propertyValue = new ArrayList< Object >();
    return convertTo( sourceObj, propertyValue );
  }
  
  @SuppressWarnings("unchecked")
  public List convertTo(S sourceObj, List propertyValues )
  {
    propertyValues.clear();
    for( Getter<S,Object> getter : getters )
    {
      propertyValues.add( getter.get(sourceObj) );
    }
    return propertyValues;
  }
  

  public void setProperties( PropertyInfo ... properties )
  {
    getters.clear();
    addProperties( properties );
  }

  public void addProperties( PropertyInfo ... properties )
  {
    for( PropertyInfo element : properties )
    {
      addProperty( element.getExpression(), element.getType() );
    }
  }

  public void setProperties( Pair<String, Class> ... properties )
  {
    getters.clear();
    addProperties( properties );
  }
  
  public void addProperties( Pair<String, Class> ... properties )
  {
    for( Pair<String,Class> element : properties )
    {
      addProperty( element.getFirst(), element.getSecond() );
    }
  }
  
  @SuppressWarnings("unchecked")
  public void addProperty( String expression, Class type )
  {
    Getter<S,Object> getter = PojoUtils.createGetter( sourceObjectClass, expression, type );
    getters.add(getter);
  }
 
}
