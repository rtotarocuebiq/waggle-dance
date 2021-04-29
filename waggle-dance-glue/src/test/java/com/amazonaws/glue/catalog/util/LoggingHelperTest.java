/**
 * Copyright (C) 2016-2021 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.glue.catalog.util;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collection;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class LoggingHelperTest {

  @Test
  public void concatCollectionToStringForLoggingTest() {
    Collection<String> logs = ImmutableList.of("test_log_1", "test_log_2", "test_log_3");
    String delimiter = "|";

    String result = LoggingHelper.concatCollectionToStringForLogging(logs, delimiter);
    String expected = "test_log_1|test_log_2|test_log_3|";

    assertThat(result, is(equalTo(expected)));
  }

  @Test
  public void concatCollectionToStringForLoggingTestWithoutCollection() {
    String delimiter = "|";

    String result = LoggingHelper.concatCollectionToStringForLogging(null, delimiter);
    String expected = "";

    assertThat(result, is(equalTo(expected)));
  }

  @Test
  public void concatCollectionToStringForLoggingTestWithoutDelimiter() {
    Collection<String> logs = ImmutableList.of("test_log_1", "test_log_2", "test_log_3");

    String result = LoggingHelper.concatCollectionToStringForLogging(logs, null);
    String expected = "test_log_1,test_log_2,test_log_3,";

    assertThat(result, is(equalTo(expected)));
  }

  @Test
  public void concatCollectionToStringForLoggingTestWithLongerThanLimitInput() {
    ImmutableList.Builder<String> listBuilder = new ImmutableList.Builder<>();

    final int max = 2000;
    final String key = "KEY";
    final StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < max; i += key.length()) {
      listBuilder.add(key);
      stringBuilder.append(key);
    }
    final String overflow = "OVERFLOW";
    for (int i = 0; i < 100; i += overflow.length()) {
      listBuilder.add(overflow);
    }

    String result = LoggingHelper.concatCollectionToStringForLogging(listBuilder.build(), "");
    String expected = stringBuilder.toString().substring(0, max);

    assertThat(result.length(), is(equalTo(max)));
    assertThat(result, is(equalTo(expected)));
    assertThat(expected.indexOf(overflow), is(equalTo(-1)));
  }
  
}
