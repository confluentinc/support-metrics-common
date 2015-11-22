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
package io.confluent.support.metrics.common;


import java.util.Properties;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class FilterTest {


  @Test
  public void testEmptyFilter() {
    Map propInput = new HashMap();
    Map propFilter = new HashMap();
    Filter f = new Filter(propFilter);

    Map out = new HashMap();
    out.putAll(f.apply(propInput));
    assertThat(out.size()).isEqualTo(0);
  }

  @Test
  public void testNullFilter() {
    Map propInput = new HashMap();
    Filter f = new Filter(null);

    Map out = new HashMap();
    out.putAll(f.apply(propInput));
    assertThat(out.size()).isEqualTo(0);
  }

  @Test
  public void testNullInput() {
    Map propFilter = new HashMap();
    Filter f = new Filter(propFilter);

    assertThat(f.apply(null)).isEqualTo(null);
  }

  @Test
  public void testInput() {
    Map propInput = new HashMap();
    propInput.put("key1", "value1");
    propInput.put("key2", "value2");
    Map propFilter = new HashMap();
    propFilter.put("key1", "value does not matter");

    Filter f = new Filter(propFilter);
    Map out = new HashMap();
    out.putAll(f.apply(propInput));

    assertThat(out.size()).isEqualTo(1);
    assertThat(out.get("key1")).isEqualTo(null);
    assertThat(out.get("key2")).isEqualTo("value2");
  }

}