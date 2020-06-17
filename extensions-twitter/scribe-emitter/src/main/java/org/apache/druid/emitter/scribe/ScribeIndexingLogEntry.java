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

import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.thrift.TBase;

public class ScribeIndexingLogEntry
{
  private String task_id;
  private String task_type;
  private String datasource;
  private String task_status;
  private String owner;
  private long duration;
  private long creation_time;

  private String role;
  private String druid_version;
  private String environment;
  private String dataCenter;
  private String cluster_name;

  public ScribeIndexingLogEntry(Event event, ScribeEmitterConfig config)
  {
    ServiceMetricEvent indexingLogEvent = (ServiceMetricEvent) event;

    this.task_id = (String) indexingLogEvent.getUserDims().getOrDefault(DruidMetrics.TASK_ID, null);
    this.task_type = (String) indexingLogEvent.getUserDims().getOrDefault(DruidMetrics.TASK_TYPE, null);
    this.datasource = (String) indexingLogEvent.getUserDims().getOrDefault(DruidMetrics.DATASOURCE, null);
    this.task_status = (String) indexingLogEvent.getUserDims().getOrDefault(DruidMetrics.TASK_STATUS, null);
    this.duration = indexingLogEvent.getValue().longValue();
    this.creation_time = indexingLogEvent.getCreatedTime().getMillis();
    this.owner = config.getRole();

    this.role = config.getRole();
    this.druid_version = config.getDruidVersion();
    this.environment = config.getEnvironment();
    this.dataCenter = config.getDataCenter();
    this.cluster_name = config.getClusterName();
  }

  public TBase toThrift()
  {
    DruidIndexingLogEvent thriftEvent = new DruidIndexingLogEvent();
    thriftEvent.setTask_id(task_id);
    thriftEvent.setTask_type(task_type);
    thriftEvent.setDatasource(datasource);
    thriftEvent.setTask_status(task_status);
    thriftEvent.setDuration(duration);
    thriftEvent.setCreation_time(creation_time);
    thriftEvent.setOwner(owner);

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
    return "task_id: " + task_id + "\n"
           + "task_type: " + task_type + "\n"
           + "datasource: " + datasource + "\n"
           + "task_status: " + task_status + "\n"
           + "duration: " + duration + "\n"
           + "creation_time: " + creation_time + "\n"
           + "owner: " + owner + "\n"
           + "role: " + role + "\n"
           + "druid_version: " + druid_version + "\n"
           + "environment: " + environment + "\n"
           + "datacenter: " + dataCenter + "\n"
           + "cluster_name: " + cluster_name + "\n";
  }

  public static ScribeIndexingLogEntry createScribeIndexingLogEntry(Event event, ScribeEmitterConfig config)
  {
    return new ScribeIndexingLogEntry(event, config);
  }
}
