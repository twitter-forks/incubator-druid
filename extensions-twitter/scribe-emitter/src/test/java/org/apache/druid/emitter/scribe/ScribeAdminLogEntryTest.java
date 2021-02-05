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
import com.twitter.logpipeline.client.EventPublisherManager;
import com.twitter.logpipeline.client.common.EventPublisher;
import com.twitter.logpipeline.client.serializers.EventLogMsgTBinarySerializer;
import com.twitter.util.Await;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.Assert;
import org.junit.Test;

public class ScribeAdminLogEntryTest
{
  @Test
  public void testRetentionChangeEntry() throws Exception
  {
    ObjectMapper objectMapper = new ObjectMapper();

    ScribeAdminLogEntry scribeEntry = new ScribeAdminLogEntry(new ServiceMetricEvent.Builder()
                                                                  .setDimension("key", "key1")
                                                                  .setDimension("type", "rules")
                                                                  .setDimension("author", "myname")
                                                                  .setDimension("comment", "Have to change the rules")
                                                                  .setDimension("remote_address", "127.0.0.1")
                                                                  .setDimension("created_date", "123456789000")
                                                                  .setDimension("payload", "key:value")
                                                                  .build("config/audit", 1)
                                                                  .build("druid/coordinator", "127.0.0.11"),
                                                              new ScribeEmitterConfig("druid_request_log", "druid_admin_log", "druid_indexing_log",
                                                                                      "iq", "test", "0.16.1-tw-0.1", "central-west", "onprem", "default-devel",
                                                                                      "org-name", "/path/to/credentials"),
                                                              objectMapper);
    String expectedResult = "audit_key: key1\n" +
        "audit_type: rules\n" +
        "author: myname\n" +
        "comment: Have to change the rules\n" +
        "remote_address: 127.0.0.1\n" +
        "created_date: 123456789000\n" +
        "payload: \"key:value\"\n" +
        "role: iq\n" +
        "druid_version: 0.16.1-tw-0.1\n" +
        "environment: test\n" +
        "datacenter: central-west\n" +
        "cluster_name: default-devel\n";
    Assert.assertEquals(expectedResult, scribeEntry.toString());

    EventPublisher<DruidAdminLogEvent> publisher =
        EventPublisherManager.buildInMemoryPublisher("test-topic",
                                                     EventLogMsgTBinarySerializer.getNewSerializer(), 1024 * 1024);
    Await.result(publisher.publish(scribeEntry.toThrift()));
  }
}
