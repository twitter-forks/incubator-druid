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
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.thrift.TBase;

public class ScribeAdminLogEntry
{
  private static final String AUDIT_KEY = "key";
  private static final String AUDIT_TYPE = "type";
  private static final String AUTHOR = "author";
  private static final String COMMENT = "comment";
  private static final String IP = "remote_address";
  private static final String CREATED_DATE = "created_date";
  private static final String PAYLOAD = "payload";

  private String audit_key;
  private String audit_type;
  private String author;
  private String comment;
  private String remote_address;
  private String created_date;
  private String payload;

  private String role;
  private String druid_version;
  private String environment;
  private String dataCenter;
  private String cluster_name;

  public ScribeAdminLogEntry(Event event, ScribeEmitterConfig config, ObjectMapper jsonMapper) throws JsonProcessingException
  {
    ServiceMetricEvent adminLogEvent = (ServiceMetricEvent) event;

    this.audit_key = (String) adminLogEvent.getUserDims().getOrDefault(AUDIT_KEY, null);
    this.audit_type = (String) adminLogEvent.getUserDims().getOrDefault(AUDIT_TYPE, null);
    this.author = (String) adminLogEvent.getUserDims().getOrDefault(AUTHOR, null);
    this.comment = (String) adminLogEvent.getUserDims().getOrDefault(COMMENT, null);
    this.remote_address = (String) adminLogEvent.getUserDims().getOrDefault(IP, null);
    this.created_date = (String) adminLogEvent.getUserDims().getOrDefault(CREATED_DATE, null);
    this.payload = jsonMapper.writeValueAsString(adminLogEvent.getUserDims().getOrDefault(PAYLOAD, null));

    this.role = config.getRole();
    this.druid_version = config.getDruidVersion();
    this.environment = config.getEnvironment();
    this.dataCenter = config.getDataCenter();
    this.cluster_name = config.getClusterName();
  }

  public TBase toThrift()
  {
    DruidAdminLogEvent thriftEvent = new DruidAdminLogEvent();
    thriftEvent.setAudit_key(audit_key);
    thriftEvent.setAudit_type(audit_type);
    thriftEvent.setAuthor(author);
    thriftEvent.setComment(comment);
    thriftEvent.setRemote_address(remote_address);
    thriftEvent.setCreated_date(created_date);
    thriftEvent.setPayload(payload);
    thriftEvent.setRole(role);
    thriftEvent.setDruid_version(druid_version);
    thriftEvent.setEnvironment(environment);
    thriftEvent.setDatacenter(dataCenter);
    thriftEvent.setCluster_name(cluster_name);
    return thriftEvent;
  }

  @Override
  public String toString()
  {
    return "audit_key: " + audit_key + "\n"
           + "audit_type: " + audit_type + "\n"
           + "author: " + author + "\n"
           + "comment: " + comment + "\n"
           + "remote_address: " + remote_address + "\n"
           + "created_date: " + created_date + "\n"
           + "payload: " + payload + "\n"
           + "role: " + role + "\n"
           + "druid_version: " + druid_version + "\n"
           + "environment: " + environment + "\n"
           + "datacenter: " + dataCenter + "\n"
           + "cluster_name: " + cluster_name + "\n";
  }

  public static ScribeAdminLogEntry createScribeAdminLogEntry(Event event, ScribeEmitterConfig config, ObjectMapper jsonMapper) throws JsonProcessingException
  {
    return new ScribeAdminLogEntry(event, config, jsonMapper);
  }
}
