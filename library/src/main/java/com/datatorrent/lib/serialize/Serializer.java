package com.datatorrent.lib.serialize;

public interface Serializer
{
  public byte[] serialize( Object obj );
  public Object deserialize( byte[] bytes );
}
