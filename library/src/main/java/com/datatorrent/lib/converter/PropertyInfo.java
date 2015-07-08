package com.datatorrent.lib.converter;

@SuppressWarnings("rawtypes")
public class PropertyInfo
{
  protected String name;
  protected String expression;
  protected Class type;

  public PropertyInfo()
  {
  }

  public PropertyInfo(String name, String expression, Class type)
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