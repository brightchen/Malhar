package com.datatorrent.contrib.util;


public class DelegateSerializer implements Serializer
{
  public static enum SERIALIZER
  {
    KRYO,
    JAVA
  };
  
  protected Serializer impl;
  
  public DelegateSerializer()
  {
    this( SERIALIZER.KRYO );
  }
  
  public DelegateSerializer( SERIALIZER serializer )
  {
    setSerializer( serializer );
  }
  
  public DelegateSerializer( Serializer impl )
  {
    this.impl = impl;
  }
  
  protected void setSerializer( SERIALIZER serializer )
  {
    switch( serializer )
    {
    case KRYO:
      impl = new KryoSerializer();
      break;
      
    case JAVA:
      impl = new JavaSerializer();
      break;
    }
  }
  
  
  @Override
  public byte[] serialize(Object obj)
  {
    return impl.serialize(obj);
  }

  @Override
  public Object deserialize(byte[] bytes)
  {
    return impl.deserialize(bytes);
  }

}
