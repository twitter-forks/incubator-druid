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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.server.log.DefaultRequestLogEventBuilderFactory;
import org.apache.druid.server.log.RequestLogEvent;
import org.junit.Assert;
import org.junit.Test;

public class ScribeRequestLogEntryTest
{
  @Test
  public void testNativeQueryEntry() throws Exception
  {
    ObjectMapper objectMapper = new ObjectMapper();
    RequestLogLine nativeLine = RequestLogLine.forNative(
        new TimeseriesQuery(
          new TableDataSource("dummy"),
          new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
          true,
          VirtualColumns.EMPTY,
          null,
          Granularities.ALL,
          ImmutableList.of(),
          ImmutableList.of(),
          5,
          ImmutableMap.of("implyDataCube", "testCube",
                          "implyFeature", "feature",
                          "implyUser", "user1",
                          "implyUserEmail", "user1@company.com",
                          "priority", 1)),
        DateTimes.of(2019, 12, 12, 3, 1),
        "127.0.0.1",
        new QueryStats(ImmutableMap.of("query/time", 13L, "query/bytes", 10L, "success", true, "identity", "allowAll"))
    );
    RequestLogEvent event = DefaultRequestLogEventBuilderFactory.instance().createRequestLogEventBuilder("feed", nativeLine).build(ImmutableMap.of("service", "broker", "host", "central-west-1"));

    ScribeRequestLogEntry scribeEntry = new ScribeRequestLogEntry(event, new ScribeEmitterConfig("druid_query_log", "druid_admin_log", "druid_indexing_log", "iq", "test", "0.16.1-tw-0.1", "central-west", "default-devel"), objectMapper);
    String expectedResult = "native_query_id: null\n" +
        "sql_query_id: null\n" +
        "role: iq\n" +
        "druid_version: 0.16.1-tw-0.1\n" +
        "environment: test\n" +
        "datacenter: central-west\n" +
        "cluster_name: default-devel\n" +
        "service_name: broker\n" +
        "host: central-west-1\n" +
        "remote_address: 127.0.0.1\n" +
        "is_sql_query: false\n" +
        "query: " + objectMapper.writeValueAsString(nativeLine.getQuery()) + "\n" +
        "datasource: dummy\n" +
        "success: true\n" +
        "creation_time: 1576119660000\n" +
        "execution_time: 13\n" +
        "output_result_size: 10\n" +
        "authenticator: allowAll\n" +
        "stats: {\"query/time\":13,\"query/bytes\":10,\"success\":true,\"identity\":\"allowAll\"}\n" +
        "imply_data_cube: testCube\n" +
        "imply_feature: feature\n" +
        "imply_user: user1\n" +
        "imply_user_email: user1@company.com\n" +
        "imply_view: null\n" +
        "imply_view_title: null\n" +
        "imply_priority: 1\n";
    Assert.assertEquals(expectedResult, scribeEntry.toString());
  }

  @Test
  public void testSqlQueryEntry() throws Exception
  {
    RequestLogLine sqlLine = RequestLogLine.forSql(
        "SELECT * FROM table LIMIT 10",
        ImmutableMap.of("", ""),
        DateTimes.of(2019, 12, 12, 3, 1),
        "127.0.0.1",
        new QueryStats(ImmutableMap.of("sqlQuery/time", 13L, "sqlQuery/bytes", 10L, "success", true, "identity", "allowAll"))
    );
    RequestLogEvent event = DefaultRequestLogEventBuilderFactory.instance().createRequestLogEventBuilder("feed", sqlLine).build(ImmutableMap.of("service", "broker", "host", "central-west-1"));

    ScribeRequestLogEntry scribeEntry = new ScribeRequestLogEntry(event, new ScribeEmitterConfig("druid_query_log", "druid_admin_log", "druid_indexing_log", "iq", "test", "0.16.1-tw-0.1", "central-west", "default-devel"), new ObjectMapper());
    String expectedResult = "native_query_id: null\n" +
        "sql_query_id: null\n" +
        "role: iq\n" +
        "druid_version: 0.16.1-tw-0.1\n" +
        "environment: test\n" +
        "datacenter: central-west\n" +
        "cluster_name: default-devel\n" +
        "service_name: broker\n" +
        "host: central-west-1\n" +
        "remote_address: 127.0.0.1\n" +
        "is_sql_query: true\n" +
        "query: SELECT * FROM table LIMIT 10\n" +
        "datasource: null\n" +
        "success: true\n" +
        "creation_time: 1576119660000\n" +
        "execution_time: 13\n" +
        "output_result_size: 10\n" +
        "authenticator: allowAll\n" +
        "stats: {\"sqlQuery/time\":13,\"sqlQuery/bytes\":10,\"success\":true,\"identity\":\"allowAll\"}\n" +
        "imply_data_cube: null\n" +
        "imply_feature: null\n" +
        "imply_user: null\n" +
        "imply_user_email: null\n" +
        "imply_view: null\n" +
        "imply_view_title: null\n" +
        "imply_priority: 0\n";
    Assert.assertEquals(expectedResult, scribeEntry.toString());
  }
}
