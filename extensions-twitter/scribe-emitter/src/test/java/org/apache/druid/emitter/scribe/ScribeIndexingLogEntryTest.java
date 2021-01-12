/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.emitter.scribe;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

public class ScribeIndexingLogEntryTest
{
  @Test
  public void testIndexingLogEntry()
  {
    DateTime now = DateTimes.nowUtc();
    ScribeIndexingLogEntry scribeEntry = new ScribeIndexingLogEntry(new ServiceMetricEvent.Builder()
                                                                      .setDimension(DruidMetrics.TASK_ID, "index_kafka_dsname")
                                                                      .setDimension(DruidMetrics.TASK_TYPE, "index_kafka")
                                                                      .setDimension(DruidMetrics.DATASOURCE, "dsname")
                                                                      .setDimension(DruidMetrics.TASK_STATUS, "SUCCESS")
                                                                      .build(now, "task/run/time", 2000300)
                                                                      .build("druid/overlord", "127.0.0.11"),
                                                                    new ScribeEmitterConfig("druid_request_log",
                                                                                             "druid_admin_log",
                                                                                           "druid_indexing_log",
                                                                                                             "iq", "test",
                                                                                                      "0.16.1-tw-0.1", "central-west",
                                                                                            "onprem", "default-devel"));
    String expectedResult = "task_id: index_kafka_dsname\n" +
        "task_type: index_kafka\n" +
        "datasource: dsname\n" +
        "task_status: SUCCESS\n" +
        "duration: 2000300\n" +
        "creation_time: " + now.getMillis() + "\n" +
        "owner: iq\n" +
        "role: iq\n" +
        "druid_version: 0.16.1-tw-0.1\n" +
        "environment: test\n" +
        "datacenter: central-west\n" +
        "cluster_name: default-devel\n";
    Assert.assertEquals(expectedResult, scribeEntry.toString());
  }
}
