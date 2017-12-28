/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.confluent.support.metrics.common.kafka;

import kafka.zk.KafkaZkClient;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import kafka.cluster.Broker;
import kafka.server.KafkaServer;
import scala.collection.JavaConversions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The tests in this class should not be run in parallel.  This limitation is caused by how the
 * current implementation of EmbeddedKafkaCLuster works.
 */
public class KafkaUtilitiesTest {

  private static final KafkaZkClient mockZkClient = mock(KafkaZkClient.class);
  private static final int anyMaxNumServers = 2;
  private static final String anyTopic = "valueNotRelevant";
  private static final int anyPartitions = 1;
  private static final int anyReplication = 1;
  private static final long anyRetentionMs = 1000L;
  private static final long oneYearRetention = 365 * 24 * 60 * 60 * 1000L;
  private static final String[] exampleTopics = {"__confluent.support.metrics", "anyTopic", "basketball"};

  @Test
  public void getNumTopicsThrowsIAEWhenKafkaZkClientIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();

    // When/Then
    try {
      kUtil.getNumTopics(null);
      fail("IllegalArgumentException expected because zkClient is null");
    } catch (NullPointerException e) {
      // ignore
    }
  }

  @Test
  public void getNumTopicsReturnsMinusOneOnError() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    KafkaZkClient zkClient = mock(KafkaZkClient.class);
    when(zkClient.getAllTopicsInCluster()).thenThrow(new RuntimeException("exception intentionally thrown by test"));

    // When/Then
    assertThat(kUtil.getNumTopics(zkClient)).isEqualTo(-1L);
  }

  @Test
  public void getNumTopicsReturnsZeroWhenThereAreNoTopics() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    KafkaZkClient zkClient = mock(KafkaZkClient.class);
    List<String> zeroTopics = new ArrayList<>();
    when(zkClient.getAllTopicsInCluster()).thenReturn(JavaConversions.asScalaBuffer(zeroTopics).toList());

    // When/Then
    assertThat(kUtil.getNumTopics(zkClient)).isEqualTo(0L);
  }

  @Test
  public void getNumTopicsReturnsCorrectNumber() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    KafkaZkClient zkClient = mock(KafkaZkClient.class);
    List<String> topics = new ArrayList<>();
    topics.add("topic1");
    topics.add("topic2");
    when(zkClient.getAllTopicsInCluster()).thenReturn(JavaConversions.asScalaBuffer(topics).toList());

    // When/Then
    assertThat(kUtil.getNumTopics(zkClient)).isEqualTo(topics.size());
  }

  @Test
  public void getBootstrapServersThrowsIAEWhenKafkaZkClientIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();

    // When/Then
    try {
      kUtil.getBootstrapServers(null, anyMaxNumServers);
      fail("IllegalArgumentException expected because zkClient is null");
    } catch (NullPointerException e) {
      // ignore
    }
  }

  @Test
  public void getBootstrapServersThrowsIAEWhenMaxNumServersIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroMaxNumServers = 0;

    // When/Then
    try {
      kUtil.getBootstrapServers(mockZkClient, zeroMaxNumServers);
      fail("IllegalArgumentException expected because max number of servers is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void getBootstrapServersReturnsEmptyListWhenThereAreNoLiveBrokers() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    KafkaZkClient zkClient = mock(KafkaZkClient.class);
    List<Broker> empty = new ArrayList<>();
    when(zkClient.getAllBrokersInCluster()).thenReturn(JavaConversions.asScalaBuffer(empty).toList());

    // When/Then
    assertThat(kUtil.getBootstrapServers(zkClient, anyMaxNumServers)).isEmpty();
  }

  @Test
  public void createTopicThrowsIAEWhenKafkaZkClientIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();

    // When/Then
    try {
      kUtil.createAndVerifyTopic(null, anyTopic, anyPartitions, anyReplication, anyRetentionMs);
      fail("IllegalArgumentException expected because zkClient is null");
    } catch (NullPointerException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenTopicIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    String nullTopic = null;

    // When/Then
    try {
      kUtil.createAndVerifyTopic(mockZkClient, nullTopic, anyPartitions, anyReplication, anyRetentionMs);
      fail("IllegalArgumentException expected because topic is null");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenTopicIsEmpty() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    String emptyTopic = "";

    // When/Then
    try {
      kUtil.createAndVerifyTopic(mockZkClient, emptyTopic, anyPartitions, anyReplication, anyRetentionMs);
      fail("IllegalArgumentException expected because topic is empty");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenNumberOfPartitionsIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroPartitions = 0;

    // When/Then
    try {
      kUtil.createAndVerifyTopic(mockZkClient, anyTopic, zeroPartitions, anyReplication, anyRetentionMs);
      fail("IllegalArgumentException expected because number of partitions is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenReplicationFactorIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroReplication = 0;

    // When/Then
    try {
      kUtil.createAndVerifyTopic(mockZkClient, anyTopic, anyPartitions, zeroReplication, anyRetentionMs);
      fail("IllegalArgumentException expected because replication factor is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void createTopicThrowsIAEWhenRetentionMsIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    long zeroRetentionMs = 0;

    // When/Then
    try {
      kUtil.createAndVerifyTopic(mockZkClient, anyTopic, anyPartitions, anyReplication, zeroRetentionMs);
      fail("IllegalArgumentException expected because retention.ms is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }


  @Test
  public void verifySupportTopicThrowsIAEWhenKafkaZkClientIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();

    // When/Then
    try {
      kUtil.verifySupportTopic(null, anyTopic, anyPartitions, anyReplication);
      fail("IllegalArgumentException expected because zkClient is null");
    } catch (NullPointerException e) {
      // ignore
    }
  }


  @Test
  public void verifySupportTopicThrowsIAEWhenTopicIsNull() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    String nullTopic = null;

    // When/Then
    try {
      kUtil.verifySupportTopic(mockZkClient, nullTopic, anyPartitions, anyReplication);
      fail("IllegalArgumentException expected because topic is null");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void verifySupportTopicThrowsIAEWhenTopicIsEmpty() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    String emptyTopic = "";

    // When/Then
    try {
      kUtil.verifySupportTopic(mockZkClient, emptyTopic, anyPartitions, anyReplication);
      fail("IllegalArgumentException expected because topic is empty");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void verifySupportTopicThrowsIAEWhenNumberOfPartitionsIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroPartitions = 0;

    // When/Then
    try {
      kUtil.verifySupportTopic(mockZkClient, anyTopic, zeroPartitions, anyReplication);
      fail("IllegalArgumentException expected because number of partitions is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void verifySupportTopicThrowsIAEWhenReplicationFactorIsZero() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    int zeroReplication = 0;

    // When/Then
    try {
      kUtil.verifySupportTopic(mockZkClient, anyTopic, anyPartitions, zeroReplication);
      fail("IllegalArgumentException expected because replication factor is zero");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void underreplicatedTopicsCanBeCreatedAndVerified() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 1;
    int partitions = numBrokers + 1;
    int replication = numBrokers + 1;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);
    KafkaZkClient zkClient = broker.zkClient();

    // When/Then
    for (String topic : exampleTopics) {
      assertThat(kUtil.createAndVerifyTopic(zkClient, topic, partitions, replication, oneYearRetention)).isTrue();
      // Only one broker is up, so the actual number of replicas will be only 1.
      assertThat(kUtil.verifySupportTopic(zkClient, topic, partitions, replication)).isEqualTo(KafkaUtilities.VerifyTopicState.Less);
    }
    assertThat(kUtil.getNumTopics(zkClient)).isEqualTo(exampleTopics.length);

    // Cleanup
    cluster.stopCluster();
    zkClient.close();
  }


  @Test
  public void underreplicatedTopicsCanBeRecreatedAndVerified() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 1;
    int partitions = numBrokers + 1;
    int replication = numBrokers + 1;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);
    KafkaZkClient zkClient = broker.zkClient();

    // When/Then
    for (String topic : exampleTopics) {
      assertThat(kUtil.createAndVerifyTopic(zkClient, topic, partitions, replication, oneYearRetention)).isTrue();
      assertThat(kUtil.createAndVerifyTopic(zkClient, topic, partitions, replication, oneYearRetention)).isTrue();
      assertThat(kUtil.verifySupportTopic(zkClient, topic, partitions, replication)).isEqualTo(KafkaUtilities.VerifyTopicState.Less);
    }
    assertThat(kUtil.getNumTopics(zkClient)).isEqualTo(exampleTopics.length);

    // Cleanup
    cluster.stopCluster();
  }

  @Test
  public void createTopicFailsWhenThereAreNoLiveBrokers() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    // Provide us with a realistic but, once the cluster is stopped, defunct instance of zkutils.
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    cluster.startCluster(1);
    KafkaServer broker = cluster.getBroker(0);
    KafkaZkClient defunctZkClient = broker.zkClient();
    cluster.stopCluster();

    // When/Then
    assertThat(kUtil.createAndVerifyTopic(defunctZkClient, anyTopic, anyPartitions, anyReplication, anyRetentionMs)).isFalse();
  }

  @Test
  public void replicatedTopicsCanBeCreatedAndVerified() {
    // Given
    KafkaUtilities kUtil = new KafkaUtilities();
    EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster();
    int numBrokers = 3;
    cluster.startCluster(numBrokers);
    KafkaServer broker = cluster.getBroker(0);
    KafkaZkClient zkClient = broker.zkClient();
    Random random = new Random();
    int replication = numBrokers;

    // When/Then
    for (String topic : exampleTopics) {
      int morePartitionsThanBrokers = random.nextInt(10) + numBrokers + 1;
      assertThat(kUtil.createAndVerifyTopic(zkClient, topic, morePartitionsThanBrokers, replication, oneYearRetention)).isTrue();
      assertThat(kUtil.verifySupportTopic(zkClient, topic, morePartitionsThanBrokers, replication)).isEqualTo(KafkaUtilities.VerifyTopicState.Exactly);
    }

    // Cleanup
    cluster.stopCluster();
  }

}
