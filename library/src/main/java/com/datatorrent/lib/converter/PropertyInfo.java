/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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