/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kevin.flink.streaming.connectors.mqtt.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

/**
 * author: kevin4936@163.com
 * 
 * 
 */

public class Retry {

  private static final Logger LOG = LoggerFactory.getLogger(Retry.class);

  /**
   * Retry invocation of given code.
   * 
   * @param attempts        Number of attempts to try executing given code. -1
   *                        represents infinity.
   * @param pauseMs         Number of backoff milliseconds.
   * @param retryExceptions Types of exceptions to retry.
   * @param callback            Function to execute.
   * @tparam A Type parameter.
   * @return Returns result of function execution or exception in case of failure.
   */
  public static <T, R> Optional<R> apply(Integer attempts, Long pauseMs, Class<Throwable>[] retryExceptions, T t,
      Function<T, Optional<R>> callback) {
    Optional<R> result = Optional.empty();
    boolean success = false;
    Integer remaining = attempts;
    while (!success && (attempts == -1 || remaining > 0)) {
      try {
        remaining -= 1;
        result = callback.apply(t);
        success = true;
      } catch (Exception e) {
        if (Arrays.asList(retryExceptions).contains(e.getClass()) && (attempts == -1 || remaining > 0)) {
          try {
            Thread.sleep(pauseMs);
          } catch (InterruptedException e1) {
            LOG.error(e1.getMessage());
          }
        } else {
          throw e;
        }
      }
    }
    return result;
  }

}
