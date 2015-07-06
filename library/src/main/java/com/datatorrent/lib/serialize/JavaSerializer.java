package com.datatorrent.lib.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class JavaSerializer// implements Serializer
{
//  @Override
//  public byte[] serialize(Object obj)
//  {
//    if( !( obj instanceof Serializable ) )
//      throw new RuntimeException( "JavaSerializer can only serialize Serializable Object." );
//    ByteArrayOutputStream bos = new ByteArrayOutputStream();
//    try {
//      ObjectOutputStream oos = new ObjectOutputStream(bos);
//      oos.writeObject(obj);
//      oos.flush();
//      return bos.toByteArray();
//    } catch (IOException ex) {
//      throw new RuntimeException(ex);
//    }
//  }
//
//  @Override
//  public Object deserialize(byte[] bytes)
//  {
//    ByteArrayInputStream bis = new ByteArrayInputStream( bytes );
//    try {
//      ObjectInputStream ois = new ObjectInputStream(bis);
//      return ois.readObject();
//    } catch (Exception ioe) {
//      throw new RuntimeException(ioe);
//    }
//  }

}
