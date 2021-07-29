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
import com.google.common.base.Joiner;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.server.log.DefaultRequestLogEvent;
import java.util.stream.Collectors;

public class ScribeRequestLogEntry
{
  private static final String SUCCESS_KEY = "success";
  private static final String IDENTITY_KEY = "identity";
  private static final String QUERY_TIME_KEY = "query/time";
  private static final String QUERY_BYTES_KEY = "query/bytes";
  private static final String SQL_QUERY_TIME_KEY = "sqlQuery/time";
  private static final String SQL_QUERY_BYTES_KEY = "sqlQuery/bytes";
  private static final String SQL_QUERY_ID_KEY = "sqlQueryId";
  private static final String NATIVE_QUERY_IDS_KEY = "nativeQueryIds";
  private static final String IMPLY_DATA_CUBE_KEY = "implyDataCube";
  private static final String IMPLY_FEATURE_KEY = "implyFeature";
  private static final String IMPLY_USER_KEY = "implyUser";
  private static final String IMPLY_USER_EMAIL_KEY = "implyUserEmail";
  private static final String IMPLY_VIEW_KEY = "implyView";
  private static final String IMPLY_VIEW_TITLE_KEY = "implyViewTitle";
  private static final String IMPLY_PRIORITY_KEY = "priority";

  private String native_query_id;
  private String sql_query_id;
  private String role;
  private String druid_version;
  private String environment;
  private String dataCenter;
  private String cluster_name;
  private String service_name;
  private String host;
  private String remote_address;
  private boolean is_sql_query;
  private String query;
  private String datasource;

  private boolean success;
  private long creation_time;
  private long execution_time;
  private long output_result_size;
  private String authenticator;
  private String stats;

  private String imply_data_cube;
  private String imply_feature;
  private String imply_user;
  private String imply_user_email;
  private String imply_view;
  private String imply_view_title;
  private int imply_priority;

  public ScribeRequestLogEntry(Event event, ScribeEmitterConfig config, ObjectMapper jsonMapper) throws JsonProcessingException
  {
    DefaultRequestLogEvent requestLogEvent = (DefaultRequestLogEvent) event;

    if (requestLogEvent.getQuery() != null) {
      this.is_sql_query = false;
      this.native_query_id = requestLogEvent.getQuery().getId();
      this.sql_query_id = requestLogEvent.getQuery().getSqlQueryId();
      this.datasource = findInnerDatasource(requestLogEvent.getQuery()).toString();
      this.query = jsonMapper.writeValueAsString(requestLogEvent.getQuery());

      this.output_result_size = (long) requestLogEvent.getQueryStats().getStats().getOrDefault(QUERY_BYTES_KEY, null);
      this.execution_time = (long) requestLogEvent.getQueryStats().getStats().getOrDefault(QUERY_TIME_KEY, null);

      this.imply_data_cube = (String) requestLogEvent.getQuery().getContextValue(IMPLY_DATA_CUBE_KEY, null);
      this.imply_feature = (String) requestLogEvent.getQuery().getContextValue(IMPLY_FEATURE_KEY, null);
      this.imply_user = (String) requestLogEvent.getQuery().getContextValue(IMPLY_USER_KEY, null);
      this.imply_user_email = (String) requestLogEvent.getQuery().getContextValue(IMPLY_USER_EMAIL_KEY, null);
      this.imply_view = (String) requestLogEvent.getQuery().getContextValue(IMPLY_VIEW_KEY, null);
      this.imply_view_title = (String) requestLogEvent.getQuery().getContextValue(IMPLY_VIEW_TITLE_KEY, null);
      this.imply_priority = (int) requestLogEvent.getQuery().getContextValue(IMPLY_PRIORITY_KEY, 0);
    } else {
      this.query = requestLogEvent.getSql();
      this.is_sql_query = true;
      this.sql_query_id = (String) requestLogEvent.getSqlQueryContext().getOrDefault(SQL_QUERY_ID_KEY, null);
      this.native_query_id = (String) requestLogEvent.getSqlQueryContext().getOrDefault(NATIVE_QUERY_IDS_KEY, null);
      this.output_result_size = (long) requestLogEvent.getQueryStats().getStats().getOrDefault(SQL_QUERY_BYTES_KEY, null);
      this.execution_time = (long) requestLogEvent.getQueryStats().getStats().getOrDefault(SQL_QUERY_TIME_KEY, null);
    }

    this.success = (boolean) requestLogEvent.getQueryStats().getStats().getOrDefault(SUCCESS_KEY, null);
    this.authenticator = (String) requestLogEvent.getQueryStats().getStats().getOrDefault(IDENTITY_KEY, null);
    this.stats = jsonMapper.writeValueAsString(requestLogEvent.getQueryStats());
    this.role = config.getRole();
    this.druid_version = config.getDruidVersion();
    this.environment = config.getEnvironment();
    this.dataCenter = config.getDataCenter();
    this.cluster_name = config.getClusterName();
    this.creation_time = requestLogEvent.getCreatedTime().getMillis();
    this.remote_address = requestLogEvent.getRemoteAddr();
    this.service_name = requestLogEvent.getService();
    this.host = requestLogEvent.getHost();
  }

  public DruidQueryLogEvent toThrift()
  {
    DruidQueryLogEvent thriftEvent = new DruidQueryLogEvent();

    thriftEvent.setIs_sql_query(is_sql_query);
    thriftEvent.setNative_query_id(native_query_id);
    thriftEvent.setSql_query_id(sql_query_id);
    thriftEvent.setDatasource(datasource);
    thriftEvent.setQuery(query);

    thriftEvent.setOutput_result_size(output_result_size);
    thriftEvent.setExecution_time(execution_time);

    thriftEvent.setImply_data_cube(imply_data_cube);
    thriftEvent.setImply_feature(imply_feature);
    thriftEvent.setImply_user(imply_user);
    thriftEvent.setImply_user_email(imply_user_email);
    thriftEvent.setImply_view(imply_view);
    thriftEvent.setImply_view_title(imply_view_title);
    thriftEvent.setImply_priority(imply_priority);

    thriftEvent.setSuccess(success);
    thriftEvent.setAuthenticator(authenticator);
    thriftEvent.setStats(stats);
    thriftEvent.setRole(role);
    thriftEvent.setDruid_version(druid_version);
    thriftEvent.setEnvironment(environment);
    thriftEvent.setDatacenter(dataCenter);
    thriftEvent.setCluster_name(cluster_name);
    thriftEvent.setCreation_time(creation_time);
    thriftEvent.setRemote_address(remote_address);
    thriftEvent.setService_name(service_name);
    thriftEvent.setHost(host);

    return thriftEvent;
  }

  @Override
  public String toString()
  {
    return "native_query_id: " + native_query_id + "\n"
            + "sql_query_id: " + sql_query_id + "\n"
            + "role: " + role + "\n"
            + "druid_version: " + druid_version + "\n"
            + "environment: " + environment + "\n"
            + "datacenter: " + dataCenter + "\n"
            + "cluster_name: " + cluster_name + "\n"
            + "service_name: " + service_name + "\n"
            + "host: " + host + "\n"
            + "remote_address: " + remote_address + "\n"
            + "is_sql_query: " + is_sql_query + "\n"
            + "query: " + query + "\n"
            + "datasource: " + datasource + "\n"

            + "success: " + success + "\n"
            + "creation_time: " + creation_time + "\n"
            + "execution_time: " + execution_time + "\n"
            + "output_result_size: " + output_result_size + "\n"
            + "authenticator: " + authenticator + "\n"
            + "stats: " + stats + "\n"

            + "imply_data_cube: " + imply_data_cube + "\n"
            + "imply_feature: " + imply_feature + "\n"
            + "imply_user: " + imply_user + "\n"
            + "imply_user_email: " + imply_user_email + "\n"
            + "imply_view: " + imply_view + "\n"
            + "imply_view_title: " + imply_view_title + "\n"
            + "imply_priority: " + imply_priority + "\n";
  }

  public static ScribeRequestLogEntry createScribeRequestLogEntry(Event event, ScribeEmitterConfig config, ObjectMapper jsonMapper) throws JsonProcessingException
  {
    return new ScribeRequestLogEntry(event, config, jsonMapper);
  }

  private Object findInnerDatasource(Query query)
  {
    DataSource _ds = query.getDataSource();
    if (_ds instanceof TableDataSource) {
      return ((TableDataSource) _ds).getName();
    }
    if (_ds instanceof QueryDataSource) {
      return findInnerDatasource(((QueryDataSource) _ds).getQuery());
    }
    if (_ds instanceof UnionDataSource) {
      return Joiner.on(",")
          .join(
              ((UnionDataSource) _ds)
                  .getDataSources()
                  .stream()
                  .map(TableDataSource::getName)
                  .collect(Collectors.toList())
          );
    } else {
      // should not come here
      return query.getDataSource();
    }
  }
}
