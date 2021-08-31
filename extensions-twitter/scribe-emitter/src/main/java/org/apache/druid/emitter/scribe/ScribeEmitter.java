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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.server.log.RequestLogEvent;

public class ScribeEmitter implements Emitter
{
  private static final String ADMIN_CONFIG = "config/audit";
  private static final String INDEXING_CONFIG = "task/run/time";
  private static final String GOOGLE_CLOUD_PLATFORM = "gcp";

  private ObjectMapper jsonMapper;

  private static Logger log = new Logger(ScribeEmitter.class);

  private final TwitterEventPublisher<DruidQueryLogEvent> requestLogScriber;
  private final TwitterEventPublisher<DruidAdminLogEvent> adminLogScriber;
  private final TwitterEventPublisher<DruidIndexingLogEvent> indexingLogScriber;
  private final ScribeEmitterConfig scribeEmitterConfig;

  public ScribeEmitter(
      ScribeEmitterConfig scribeEmitterConfig,
      ObjectMapper jsonMapper
  )
  {
    this.requestLogScriber = new TwitterEventPublisher(scribeEmitterConfig.getRequestLogScribeCategory(), isDatacenterHostGCP(scribeEmitterConfig.getDataCenterHost()), scribeEmitterConfig.getGcpOrgName(), scribeEmitterConfig.getGcpCredentialsPath());
    this.adminLogScriber = new TwitterEventPublisher(scribeEmitterConfig.getAdminLogScribeCategory(), isDatacenterHostGCP(scribeEmitterConfig.getDataCenterHost()), scribeEmitterConfig.getGcpOrgName(), scribeEmitterConfig.getGcpCredentialsPath());
    this.indexingLogScriber = new TwitterEventPublisher(scribeEmitterConfig.getIndexingLogScribeCategory(), isDatacenterHostGCP(scribeEmitterConfig.getDataCenterHost()), scribeEmitterConfig.getGcpOrgName(), scribeEmitterConfig.getGcpCredentialsPath());
    this.scribeEmitterConfig = scribeEmitterConfig;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void start()
  {

  }

  @Override
  public void emit(Event event)
  {
    if (event instanceof RequestLogEvent) {
      try {
        requestLogScriber.scribe(ScribeRequestLogEntry.createScribeRequestLogEntry(event, scribeEmitterConfig, jsonMapper).toThrift());
      }
      catch (JsonProcessingException e) {
        log.warn("" + e + " Could not process native query string as JSON object " + event);
      }
      catch (Exception e) {
        log.warn("" + e + ", Could not emit event: " + event);
      }
    } else if (event instanceof ServiceMetricEvent && ((ServiceMetricEvent) event).getMetric().compareTo(ADMIN_CONFIG) == 0) {
      try {
        adminLogScriber.scribe(ScribeAdminLogEntry.createScribeAdminLogEntry(event, scribeEmitterConfig, jsonMapper).toThrift());
      }
      catch (JsonProcessingException e) {
        log.warn("" + e + " Could not process administrative string as JSON object " + event);
      }
      catch (Exception e) {
        log.warn("" + e + ", Could not emit event: " + event);
      }
    } else if (event instanceof ServiceMetricEvent && ((ServiceMetricEvent) event).getMetric().compareTo(INDEXING_CONFIG) == 0) {
      try {
        indexingLogScriber.scribe(ScribeIndexingLogEntry.createScribeIndexingLogEntry(event, scribeEmitterConfig).toThrift());
      }
      catch (Exception e) {
        log.warn("" + e + ", Could not emit event: " + event);
      }
    }
  }

  @Override
  public void flush()
  {
  }

  @Override
  public void close()
  {
  }

  private boolean isDatacenterHostGCP(String datacenterHost)
  {
    return GOOGLE_CLOUD_PLATFORM.equals(datacenterHost);
  }
}
