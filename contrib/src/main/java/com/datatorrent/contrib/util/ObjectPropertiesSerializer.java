package com.datatorrent.contrib.util;


/**
 * 
 * Instead of serialize/deserialize whole object, this Serializer serialize the properties of object 
 *
 */
public class ObjectPropertiesSerializer<T> extends DelegateSerializer
{
  private ObjectPropertiesConverter converter;
  
  public ObjectPropertiesSerializer( Class<T> type )
  {
    useSerializer( SerializerImplementer.KRYO );
    useConverter( new ObjectSequentialPropertiesConverter( type ) );
  }
  
  public ObjectPropertiesSerializer useSerializer( SerializerImplementer serializer )
  {
    setSerializer(serializer);
    return this;
  }
  
  public ObjectPropertiesSerializer useSerializer( Serializer serializer )
  {
    impl = serializer;
    return this;
  }
  
  public ObjectPropertiesSerializer useConverter( ObjectPropertiesConverter converter )
  {
    this.converter = converter;
    return this;
  }

  @Override
  public byte[] serialize(Object obj)
  {
    return impl.serialize( converter.fromObject(obj) );
  }

  @Override
  public Object deserialize(byte[] bytes)
  {
    return converter.toObject( impl.deserialize(bytes) );
  }
}
