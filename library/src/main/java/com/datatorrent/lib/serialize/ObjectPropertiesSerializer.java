package com.datatorrent.lib.serialize;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.netlet.util.Slice;


/**
 * 
 * Instead of serialize/deserialize whole object, this Serializer serialize the properties of object 
 *
 */
public class ObjectPropertiesSerializer<O, C>  extends AbstractSerializer<C> implements StreamCodec<O>
{
  private ObjectPropertiesConverter<O, C> converter;
  
  public ObjectPropertiesSerializer( Class<O> type )
  {
    useImplementer( new KryoSerializableStreamCodec<C>() );
    useConverter( new ObjectSequentialPropertiesConverter( type ) );
  }
  
  public ObjectPropertiesSerializer<O,C> useImplementer( StreamCodec<C> implementer )
  {
    setImplementer(implementer);
    return this;
  }
  

  public ObjectPropertiesSerializer<O,C> useConverter( ObjectPropertiesConverter<O, C> converter )
  {
    this.converter = converter;
    return this;
  }


  @Override
  public Object fromByteArray(Slice fragment)
  {
    return converter.toOriginObject( (C)implementer.fromByteArray(fragment) );
  }


  @Override
  public Slice toByteArray(O o)
  {
    return implementer.toByteArray( converter.fromOriginObject(o));
  }

  @Override
  public int getPartition(O o)
  {
    return implementer.getPartition( converter.fromOriginObject(o) );
  }
  
}
