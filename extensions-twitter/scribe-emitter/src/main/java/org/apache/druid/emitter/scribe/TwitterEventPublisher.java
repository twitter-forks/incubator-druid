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

import com.twitter.logpipeline.client.EventPublisherManager;
import com.twitter.logpipeline.client.common.EventLogMessage;
import com.twitter.logpipeline.client.common.EventPublisher;
import com.twitter.logpipeline.client.serializers.EventLogMsgTBinarySerializer;
import com.twitter.util.Await;
import com.twitter.util.Future;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;

public class TwitterEventPublisher<T>
{
  private static final String GCP_ORG_NAME = "twttr-dp-org-ie";
  private static final String GCP_CREDENTIALS_PATH = "/var/lib/tss/keys/druid-albus/cloud/gcp/dp/shadow.json";
  private EventPublisher<T> publisher;

  private static Logger log = new Logger(TwitterEventPublisher.class);

  public TwitterEventPublisher(String scribeCategory, boolean isDatacenterHostGCP, String gcpOrgName, String gcpCredentialsPath)
  {
    if (isDatacenterHostGCP) {
      final String logCategoryName = "projects/" + gcpOrgName + "/topics/" + scribeCategory;
      try {
        publisher = (EventPublisher<T>) EventPublisherManager.buildGcpLogPipelinePublisher(
            logCategoryName,
            EventLogMsgTBinarySerializer.getNewSerializer(),
            gcpCredentialsPath);
      }
      catch (IOException e) {
        log.error("Failed to create scribe emitter for" + logCategoryName);
      }
    } else {
      try {
        publisher = (EventPublisher<T>) EventPublisherManager.buildScribeLogPipelinePublisher(
            scribeCategory,
            EventLogMsgTBinarySerializer.getNewSerializer());
      }
      catch (IOException e) {
        log.error("Failed to create scribe emitter for" + scribeCategory);
      }
    }
  }

  public void scribe(T thriftMessage)
      throws Exception
  {
    // Build Event log message and publish the event asynchronously
    EventLogMessage<T> message = EventLogMessage.buildEventLogMessage(publisher.getLogCategoryName(), thriftMessage);
    if (publisher != null) {
      Future<String> future = publisher.publish(message);
      Await.result(future);
    }
  }
}
