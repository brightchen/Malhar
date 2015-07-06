package com.datatorrent.lib.serialize;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.serialize.ObjectPropertiesConverter.PropertyInfo;

public class SerializerFactory
{
  @SuppressWarnings({ "rawtypes", "unchecked" }) 
  public static <T> StreamCodec<T> getSerializer( Class<T> type, StreamCodec streamCodec, PropertyInfo[] propertyInfos )
  {
    if( propertyInfos == null || propertyInfos.length == 0 || type == null )
      return new DelegateSerializer<T>( streamCodec );
    else
    {
      ObjectSequentialPropertiesConverter converter = new ObjectSequentialPropertiesConverter(type);
      converter.setProperties(propertyInfos);
      return new ObjectPropertiesSerializer<T,Object>( type ).useImplementer(streamCodec).useConverter( converter );
    }
  }
}
