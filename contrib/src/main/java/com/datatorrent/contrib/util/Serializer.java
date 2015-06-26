package com.datatorrent.contrib.util;

public interface Serializer
{
  public byte[] serialize( Object obj );
  public Object deserialize( byte[] bytes );
}
