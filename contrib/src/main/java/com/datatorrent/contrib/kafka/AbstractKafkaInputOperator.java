/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.kafka;

import java.util.HashSet;
import java.util.Set;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import kafka.message.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.Operator.CheckpointListener;

/**
 * This is a base implementation of a Kafka input operator, which consumes data from Kafka message bus.&nbsp;
 * Subclasses should implement the method for emitting tuples to downstream operators.
 * <p>
 * Properties:<br>
 * <b>tuplesBlast</b>: Number of tuples emitted in each burst<br>
 * <b>bufferSize</b>: Size of holding buffer<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method emitTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 *
 * Shipped jars with this operator:<br>
 * <b>kafka.javaapi.consumer.SimpleConsumer.class</b> Official kafka consumer client <br>
 * <b>org.I0Itec.zkclient.ZkClient.class</b>  Kafka client depends on this <br>
 * <b>scala.ScalaObject.class</b>  Kafka client depends on this <br>
 * <b>com.yammer.matrics.Metrics.class</b>   Kafka client depends on this <br> <br>
 *
 * Each operator can only consume 1 topic<br>
 * If you want partitionable operator refer to {@link AbstractPartitionableKafkaInputOperator}
 * <br>
 * </p>
 *
 * @displayName Abstract Kafka Input
 * @category Messaging
 * @tags input operator
 *
 * @since 0.3.2
 */
//SimpleConsumer is kafka consumer client used by this operator, zkclient is used by high-level kafka consumer
public abstract class AbstractKafkaInputOperator<K extends KafkaConsumer> implements InputOperator, ActivationListener<OperatorContext>, CheckpointListener
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaInputOperator.class);

  @Min(1)
  private int maxTuplesPerWindow = Integer.MAX_VALUE;
  private transient int emitCount = 0;
  @NotNull
  @Valid
  protected KafkaConsumer consumer = new SimpleKafkaConsumer();

  /**
   * Any concrete class derived from KafkaInputOperator has to implement this method
   * so that it knows what type of message it is going to send to Malhar in which output port.
   *
   * @param message
   */
  protected abstract void emitTuple(Message message);

  public int getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(int maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    logger.debug("consumer {} topic {} cacheSize {}", consumer, consumer.getTopic(), consumer.getCacheSize());
    consumer.create();
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    consumer.teardown();
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    emitCount = 0;
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
    // commit the kafka consumer offset
    getConsumer().commitOffset();
  }

  @Override
  public void committed(long windowId)
  {
  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void activate(OperatorContext ctx)
  {
    // Don't start thread here!
    // Because how many threads we want to start for kafka consumer depends on the type of kafka client and the message metadata(topic/partition/replica)
    consumer.start();
  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void deactivate()
  {
    consumer.stop();
  }

  /**
   * Implement InputOperator Interface.
   */
  @Override
  public void emitTuples()
  {
    int count = consumer.messageSize();
    if (maxTuplesPerWindow > 0) {
      count = Math.min(count, maxTuplesPerWindow - emitCount);
    }
    for (int i = 0; i < count; i++) {
      emitTuple(consumer.pollMessage());
    }
    emitCount += count;
  }

  public void setConsumer(K consumer)
  {
    this.consumer = consumer;
  }

  public KafkaConsumer getConsumer()
  {
    return consumer;
  }

  //add topic as operator property
  public void setTopic(String topic)
  {
    this.consumer.setTopic(topic);
  }

  //add brokerlist as operator property
  public void setBrokerSet(String brokerString)
  {
    Set<String> brokerSet = new HashSet<String>();
    for (String broker : brokerString.split(",")) {
      brokerSet.add(broker);
    }
    this.consumer.setBrokerSet(brokerSet);
  }

}
