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
import java.util.HashMap;
import java.util.Map;

public class Filter {

  private Map<String, Boolean> filterMap;

  /**
   * Constructor with a filter map.
   * @param filterMap A Key-Value mapping. If key exists, there is a match. Value is ignored
   */
  public Filter(Map<String, Boolean> filterMap) {
    this.filterMap = filterMap;
  }

  /**
   * Apply the filter to an input
   * @param input: Input data
   * @return Input data minus any element that is filtered out
   */
  public Map apply(Map input) {
    if (input == null) {
      return null;
    }

    Map retMap = new HashMap();
    retMap.putAll(input);

    if (filterMap == null) return retMap;


    for (Object key: filterMap.keySet()) {
      if (retMap.containsKey(key)) {
        retMap.remove(key);
      }
    }

    return retMap;
  }
}
