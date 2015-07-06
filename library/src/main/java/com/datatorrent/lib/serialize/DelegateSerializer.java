package com.datatorrent.lib.serialize;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.netlet.util.Slice;


public class DelegateSerializer<T> extends AbstractSerializer<T> implements StreamCodec<T>
{
  public DelegateSerializer()
  {
  }
  
  public DelegateSerializer( StreamCodec<T> implementer )
  {
    setImplementer( implementer );
  }
  

  public Object fromByteArray(Slice fragment)
  {
    return implementer.fromByteArray(fragment);
  }


  @Override
  public Slice toByteArray(T o)
  {
    return implementer.toByteArray(o);
  }

  @Override
  public int getPartition(T o)
  {
    return implementer.getPartition(o);
  }

}
