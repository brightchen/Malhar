package com.datatorrent.lib.serialize;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.netlet.util.Slice;

public abstract class AbstractSerializer<S>
{
  protected StreamCodec<S> implementer;

  public void setImplementer( StreamCodec<S> implementer )
  {
    this.implementer = implementer;
  }
  

  public Object fromByteArray(Slice fragment)
  {
    return implementer.fromByteArray(fragment);
  }

}