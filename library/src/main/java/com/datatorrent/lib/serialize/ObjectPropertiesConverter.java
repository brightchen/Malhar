package com.datatorrent.lib.serialize;


/**
 * 
 * This interface provides methods to convert Object to another Object which represented by its properties
 *
 * @param <O> the type of the object
 * @param <C> the type of the object converted to
 */
public interface ObjectPropertiesConverter< O, C>
{
  public C fromOriginObject( O obj );
  
  /**
   * This method same as <code>fromObject( Object obj )</code>. 
   * just give a chance to reuse the propertyValues object, or use sub-type of T
   * @param value
   * @param propertyValues
   * @return
   */
  public C fromOriginObject( O value, C propertyValues );
  
  public O toOriginObject( C propertyValues );
  
  public O toOriginObject( C propertyValues, O value );
  
  

  
  public static class PropertyInfo
  {
    protected String name;
    protected String expression;
    protected Class type;
    
    public PropertyInfo(){}
    
    public PropertyInfo( String name, String expression, Class type )
    {
      this.name = name;
      this.expression = expression;
      this.type = type;
    }
    

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public String getExpression()
    {
      return expression;
    }

    public void setExpression(String expression)
    {
      this.expression = expression;
    }

    public Class getType()
    {
      return type;
    }

    public void setType(Class type)
    {
      this.type = type;
    }

    /**
     * the columnName should not duplicate( case-insensitive )
     */
    @Override
    public int hashCode()
    {
      return name.toLowerCase().hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null || !(obj instanceof PropertyInfo))
        return false;
      return name.equalsIgnoreCase(((PropertyInfo) obj).name);
    }
  }
}
