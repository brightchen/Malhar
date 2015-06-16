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
package com.datatorrent.contrib.kinesis;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;

/**
 *
 */
public abstract class KinesisOutputOperatorTest< O extends AbstractKinesisOutputOperator, G extends Operator > extends KinesisOperatorTestBase
{
  private static final Logger logger = LoggerFactory.getLogger(KinesisOutputOperatorTest.class);
  protected static int tupleCount = 0;
  protected static final int maxTuple = 20;
  private static CountDownLatch latch;

  @Before
  public void beforeTest()
  {
    shardCount = 1;
    super.beforeTest();
  }

  /**
   * Test AbstractKinesisOutputOperator (i.e. an output adapter for Kinesis, aka producer).
   * This module sends data into an ActiveMQ message bus.
   *
   * [Generate tuple] ==> [send tuple through Kinesis output adapter(i.e. producer) into Kinesis message bus]
   * ==> [receive data in outside Kinesis listener (i.e consumer)]
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings({"SleepWhileInLoop", "empty-statement", "rawtypes"})
  public void testKinesieOutputOperator() throws Exception
  {
    //initialize the latch to synchronize the threads
    latch = new CountDownLatch(maxTuple);
    // Setup a message listener to receive the message
    KinesisTestConsumer listener = new KinesisTestConsumer(streamName);
    listener.setLatch(latch);
    new Thread(listener).start();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    DAG dag = lma.getDAG();

    // Create ActiveMQStringSinglePortOutputOperator
    G generator = addGenerateOperator( dag );
    
    O node = addTestingOperator( dag );
    //KinesisStringOutputOperator node = dag.addOperator("KinesisMessageProducer", KinesisStringOutputOperator.class);
    node.setAccessKey(credentials.getCredentials().getAWSSecretKey());
    node.setSecretKey(credentials.getCredentials().getAWSAccessKeyId());
    node.setBatchSize(500);

    node.setStreamName(streamName);

    // Connect ports
    dag.addStream("Kinesis message", getOutputPortOfGenerator( generator ), node.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    Configuration conf = new Configuration(false);
    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    // Immediately return unless latch timeout in 5 seconds
    latch.await(15, TimeUnit.SECONDS);
    lc.shutdown();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", tupleCount, listener.holdingBuffer.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.holdingBuffer.size()));

    listener.close();
  }

  protected abstract G addGenerateOperator( DAG dag );
  protected abstract DefaultOutputPort getOutputPortOfGenerator( G generator );
  protected abstract O addTestingOperator( DAG dag );

}