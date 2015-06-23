package com.datatorrent.contrib.util;

import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoSerializer implements Serializer
{
  private Kryo kryo = new Kryo();
      
  @Override
  public byte[] serialize(Object obj)
  {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);

    kryo.writeClassAndObject(output, obj);
    output.flush();
    //output.toBytes() is empty.
    return os.toByteArray();
  }

  @Override
  public Object deserialize(byte[] bytes)
  {
    return kryo.readClassAndObject( new Input( bytes ) );
  }
}
