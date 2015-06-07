/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.partitioner;

import com.datatorrent.api.Operator;

/**
 * This is a simple partitioner which creates partitionCount number of clones of an operator.
 * @param <T> The type of the operator
 *
 * @since 2.0.0
 */
@Deprecated
public class StatelessPartitioner<T extends Operator> extends com.datatorrent.common.partitioner.StatelessPartitioner<T>
{
  private static final long serialVersionUID = 201411071710L;

  /**
   * This creates a partitioner which creates only one partition.
   */
  public StatelessPartitioner()
  {
  }

  /**
   * This constructor is used to create the partitioner from a property.
   * @param value A string which is an integer of the number of partitions to create
   */
  public StatelessPartitioner(String value)
  {
    this(Integer.parseInt(value));
  }

  /**
   * This creates a partitioner which creates partitonCount partitions.
   * @param partitionCount The number of partitions to create.
   */
  public StatelessPartitioner(int partitionCount)
  {
    super(partitionCount);
  }

}
