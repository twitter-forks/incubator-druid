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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ScribeEmitterConfig
{
  @JsonProperty
  private final String requestLogScribeCategory;

  @JsonProperty
  private final String role;

  @JsonProperty
  private final String environment;

  @JsonProperty
  private final String druidVersion;

  @JsonProperty
  private final String dataCenter;

  @JsonProperty
  private final String clusterName;

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ScribeEmitterConfig)) {
      return false;
    }

    ScribeEmitterConfig that = (ScribeEmitterConfig) o;

    if (!getRequestLogScribeCategory().equals(that.getRequestLogScribeCategory())) {
      return false;
    }
    if (!getRole().equals(that.getRole())) {
      return false;
    }
    if (!getEnvironment().equals(that.getEnvironment())) {
      return false;
    }
    if (!getDruidVersion().equals(that.getDruidVersion())) {
      return false;
    }
    if (!getDataCenter().equals(that.getDataCenter())) {
      return false;
    }
    if (!getClusterName().equals(that.getClusterName())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = getRequestLogScribeCategory().hashCode();
    result = 31 * result + getRole().hashCode();
    result = 31 * result + getEnvironment().hashCode();
    result = 31 * result + getDruidVersion().hashCode();
    result = 31 * result + getDataCenter().hashCode();
    result = 31 * result + getClusterName().hashCode();
    return result;
  }

  @JsonCreator
  public ScribeEmitterConfig(
      @JsonProperty("requestLogScribeCategory") String requestLogScribeCategory,
      @JsonProperty("role") String role,
      @JsonProperty("environment") String environment,
      @JsonProperty("druidVersion") String druidVersion,
      @JsonProperty("dataCenter") String dataCenter,
      @JsonProperty("clusterName") String clusterName
  )
  {
    this.requestLogScribeCategory = requestLogScribeCategory;
    this.role = role;
    this.environment = environment;
    this.druidVersion = druidVersion;
    this.dataCenter = dataCenter;
    this.clusterName = clusterName;
  }

  @JsonProperty
  public String getRequestLogScribeCategory()
  {
    return requestLogScribeCategory;
  }

  @JsonProperty
  public String getRole()
  {
    return role;
  }

  @JsonProperty
  public String getEnvironment()
  {
    return environment;
  }

  @JsonProperty
  public String getDruidVersion()
  {
    return druidVersion;
  }

  @JsonProperty
  public String getDataCenter()
  {
    return dataCenter;
  }

  @JsonProperty
  public String getClusterName()
  {
    return clusterName;
  }
}
