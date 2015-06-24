package com.datatorrent.contrib.util;

import com.datatorrent.contrib.util.DelegateSerializer.SerializerImplementer;
import com.datatorrent.contrib.util.ObjectPropertiesConverter.PropertyInfo;

public class SerializerFactory
{
  public static Serializer getSerializer( Class type, SerializerImplementer serializerImplementer, PropertyInfo[] propertyInfos )
  {
    if( propertyInfos == null || propertyInfos.length == 0 )
      return new DelegateSerializer( serializerImplementer );
    else
    {
      ObjectSequentialPropertiesConverter converter = new ObjectSequentialPropertiesConverter(type);
      converter.setProperties(propertyInfos);
      return new ObjectPropertiesSerializer( type ).useSerializer(serializerImplementer).useConverter( converter );
    }
  }
}
