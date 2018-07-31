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
package io.confluent.support.metrics;

import com.google.common.collect.ObjectArrays;

import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import io.confluent.support.metrics.utils.CustomerIdExamples;
import kafka.server.KafkaConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BaseSupportConfigTest {

  @Test
  public void testValidCustomer() {
    for (String validId : CustomerIdExamples.validCustomerIds) {
      assertThat(validId + " is an invalid customer identifier",
          BaseSupportConfig.isConfluentCustomer(validId), is(true)
      );
    }
  }

  @Test
  public void testValidNewCustomer() {
    String[]
        validNewCustomerIds =
        ObjectArrays.concat(CustomerIdExamples.validCaseSensitiveNewCustomerIds,
                            CustomerIdExamples.validCaseInsensitiveNewCustomerIds,
                            String.class
        );
    for (String validId : validNewCustomerIds) {
      assertThat(validId + " is an invalid new customer identifier",
                 BaseSupportConfig.isConfluentCustomer(validId), is(true)
      );
    }
  }

  @Test
  public void testInvalidCustomer() {
    String[]
        invalidIds =
        ObjectArrays.concat(CustomerIdExamples.invalidCustomerIds,
            CustomerIdExamples.validAnonymousIds,
            String.class
        );
    for (String invalidCustomerId : invalidIds) {
      assertThat(invalidCustomerId + " is a valid customer identifier",
          BaseSupportConfig.isConfluentCustomer(invalidCustomerId), is(false)
      );
    }
  }

  @Test
  public void testValidAnonymousUser() {
    for (String validId : CustomerIdExamples.validAnonymousIds) {
      assertThat(validId + " is an invalid anonymous user identifier",
          BaseSupportConfig.isAnonymousUser(validId), is(true)
      );
    }
  }

  @Test
  public void testInvalidAnonymousUser() {
    String[]
        invalidIds =
        ObjectArrays.concat(CustomerIdExamples.invalidAnonymousIds,
            CustomerIdExamples.validCustomerIds,
            String.class
        );
    for (String invalidId : invalidIds) {
      assertThat(invalidId + " is a valid anonymous user identifier",
          BaseSupportConfig.isAnonymousUser(invalidId), is(false)
      );
    }
  }

  @Test
  public void testCustomerIdValidSettings() {
    String[]
        validValues =
        ObjectArrays.concat(CustomerIdExamples.validAnonymousIds,
            CustomerIdExamples.validCustomerIds,
            String.class
        );
    for (String validValue : validValues) {
      assertThat(validValue + " is an invalid value for " + BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
          BaseSupportConfig.isSyntacticallyCorrectCustomerId(validValue),
          is(true)
      );
      // old customer Ids are all case-insensitive
      assertThat(validValue + " is case-sensitive customer ID.",
                 BaseSupportConfig.isCaseSensitiveCustomerId(validValue),
                 is(false)
      );
    }
  }

  @Test
  public void testCustomerIdInvalidSettings() {
    String[]
        invalidValues =
        ObjectArrays.concat(CustomerIdExamples.invalidAnonymousIds,
            CustomerIdExamples.invalidCustomerIds,
            String.class
        );
    for (String invalidValue : invalidValues) {
      assertThat(invalidValue + " is a valid value for " + BaseSupportConfig
              .CONFLUENT_SUPPORT_CUSTOMER_ID_CONFIG,
          BaseSupportConfig.isSyntacticallyCorrectCustomerId(invalidValue), is(false)
      );
    }
  }

  @Test
  public void testCaseInsensitiveNewCustomerIds() {
    for (String validValue : CustomerIdExamples.validCaseInsensitiveNewCustomerIds) {
      assertThat(validValue + " is case-sensitive customer ID.",
                 BaseSupportConfig.isCaseSensitiveCustomerId(validValue),
                 is(false)
      );
    }
  }

  @Test
  public void testCaseSensitiveNewCustomerIds() {
    for (String validValue : CustomerIdExamples.validCaseSensitiveNewCustomerIds) {
      assertThat(validValue + " is case-insensitive customer ID.",
                 BaseSupportConfig.isCaseSensitiveCustomerId(validValue),
                 is(true)
      );
    }
  }

  @Test
  public void proactiveSupportConfigIsValidKafkaConfig() throws IOException {
    // Given
    Properties brokerConfiguration = defaultBrokerConfiguration();

    // When
    KafkaConfig cfg = KafkaConfig.fromProps(brokerConfiguration);

    // Then
    assertThat(cfg.brokerId()).isEqualTo(0);
    assertThat(cfg.zkConnect()).startsWith("localhost:");
  }

  private Properties defaultBrokerConfiguration() throws IOException {
    Properties brokerConfiguration = new Properties();
    brokerConfiguration.load(BaseSupportConfigTest.class.getResourceAsStream("/default-server"
                                                                             + ".properties"));
    return brokerConfiguration;
  }

  @Test
  public void canParseProactiveSupportConfiguration() throws IOException {
    // Given
    Properties brokerConfiguration = defaultBrokerConfiguration();

    BaseSupportConfig supportConfig = new TestSupportConfig(brokerConfiguration);
    // When/Then
    assertThat(supportConfig.getMetricsEnabled()).isEqualTo(true);
    assertThat(supportConfig.getCustomerId()).isEqualTo("c0");
    assertThat(supportConfig.isProactiveSupportEnabled()).isTrue();

  }


  @Test
  public void testGetDefaultProps() {
    // Given
    Properties overrideProps = new Properties();
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertThat(supportConfig.getMetricsEnabled()).isEqualTo(true);
    assertThat(supportConfig.getCustomerId()).isEqualTo("anonymous");
    assertThat(supportConfig.getReportIntervalMs()).isEqualTo(24 * 60 * 60 * 1000);
    assertThat(supportConfig.getKafkaTopic()).isEqualTo("__confluent.support.metrics");
    assertThat(supportConfig.isHttpEnabled()).isEqualTo(true);
    assertThat(supportConfig.isHttpsEnabled()).isEqualTo(true);
    assertThat(supportConfig.isProactiveSupportEnabled()).isTrue();
    assertThat(supportConfig.getProxy().equals(""));
    assertThat(supportConfig.getEndpointHTTP()).isEqualTo("http://support-metrics.confluent"
                                                          + ".io/anon");
    assertThat(supportConfig.getEndpointHTTPS()).isEqualTo("https://support-metrics.confluent"
                                                           + ".io/anon");
  }

  @Test
  public void testMergeAndValidatePropsFilterDisallowedKeys() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_CONFIG,
        "anyValue"
    );
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_CONFIG,
        "anyValue"
    );
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertThat(supportConfig.getEndpointHTTP()).isEqualTo("http://support-metrics.confluent"
                                                          + ".io/anon");
    assertThat(supportConfig.getEndpointHTTPS()).isEqualTo("https://support-metrics.confluent"
                                                           + ".io/anon");
  }

  @Test
  public void testMergeAndValidatePropsDisableEndpoints() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG, "false");
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "false");
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertThat(supportConfig.getEndpointHTTP()).isEmpty();
    assertThat(supportConfig.getEndpointHTTPS()).isEmpty();
  }

  @Test
  public void testOverrideReportInterval() {
    // Given
    Properties overrideProps = new Properties();
    int reportIntervalHours = 1;
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_REPORT_INTERVAL_HOURS_CONFIG,
        String.valueOf(reportIntervalHours)
    );
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertThat(supportConfig.getReportIntervalMs()).isEqualTo(reportIntervalHours * 60 * 60 * 1000);
  }

  @Test
  public void testOverrideTopic() {
    // Given
    Properties overrideProps = new Properties();
    overrideProps.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG,
        "__another_example_topic"
    );
    // When
    BaseSupportConfig supportConfig = new TestSupportConfig(overrideProps);

    // Then
    assertThat(supportConfig.getKafkaTopic()).isEqualTo("__another_example_topic");
  }

  @Test
  public void isProactiveSupportEnabledFull() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "true");

    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertThat(supportConfig.isProactiveSupportEnabled()).isTrue();
  }

  @Test
  public void isProactiveSupportDisabledFull() {
    // Given
    Properties serverProperties = new Properties();

    serverProperties
        .setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENABLE_CONFIG, "false");
    serverProperties
        .setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "anyTopic");
    serverProperties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG,
        "true"
    );
    serverProperties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG,
        "true"
    );
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertThat(supportConfig.isProactiveSupportEnabled()).isFalse();
  }

  @Test
  public void isProactiveSupportEnabledTopicOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties
        .setProperty(BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_TOPIC_CONFIG, "anyTopic");
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertThat(supportConfig.isProactiveSupportEnabled()).isTrue();
  }

  @Test
  public void isProactiveSupportEnabledHTTPOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_INSECURE_ENABLE_CONFIG,
        "true"
    );
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertThat(supportConfig.isProactiveSupportEnabled()).isTrue();
  }

  @Test
  public void isProactiveSupportEnabledHTTPSOnly() {
    // Given
    Properties serverProperties = new Properties();
    serverProperties.setProperty(
        BaseSupportConfig.CONFLUENT_SUPPORT_METRICS_ENDPOINT_SECURE_ENABLE_CONFIG, "true");
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertThat(supportConfig.isProactiveSupportEnabled()).isTrue();
  }

  @Test
  public void proactiveSupportIsDisabledByDefaultWhenBrokerConfigurationIsEmpty() {
    // Given
    Properties serverProperties = new Properties();
    BaseSupportConfig supportConfig = new TestSupportConfig(serverProperties);
    // When/Then
    assertThat(supportConfig.isProactiveSupportEnabled()).isTrue();
  }

  public class TestSupportConfig extends BaseSupportConfig {

    public TestSupportConfig(Properties originals) {
      super(originals);
    }

    @Override
    protected String getAnonymousEndpoint(boolean secure) {
      if (!secure) {
        return "http://support-metrics.confluent.io/anon";
      } else {
        return "https://support-metrics.confluent.io/anon";
      }
    }

    @Override
    protected String getTestEndpoint(boolean secure) {
      if (!secure) {
        return "http://support-metrics.confluent.io/test";
      } else {
        return "https://support-metrics.confluent.io/test";
      }
    }

    @Override
    protected String getCustomerEndpoint(boolean secure) {
      if (!secure) {
        return "http://support-metrics.confluent.io/submit";
      } else {
        return "https://support-metrics.confluent.io/submit";
      }
    }
  }

}
