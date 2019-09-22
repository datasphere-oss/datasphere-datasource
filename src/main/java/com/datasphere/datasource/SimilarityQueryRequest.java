/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datasphere.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.datasphere.datasource.data.AbstractQueryRequest;
import com.datasphere.datasource.data.QueryRequest;
import com.datasphere.server.common.exception.BadRequestException;
import com.datasphere.server.domain.workbook.configurations.datasource.DataSource;

/**
 * "Similarity" Request object for query
 *
 * @since 1.1
 */
public class SimilarityQueryRequest extends AbstractQueryRequest implements QueryRequest, Serializable {

  /**
   * Name of data source to query
   *
   */
  List<String> dataSources;

  public SimilarityQueryRequest() {
    // Empty Constructor
  }

  @JsonCreator
  public SimilarityQueryRequest(@JsonProperty("dataSources") List<String> dataSources,
                                @JsonProperty("context") Map<String, Object> context) throws BadRequestException {

    if(dataSources == null || dataSources.size() != 2) {
      throw new BadRequestException("2 datasource names are required.");
    }

    super.context = context;

    this.dataSources = dataSources;
  }

  public List<String> getDataSources() {
    return dataSources;
  }

  @Override
  public DataSource getDataSource() {
    return null;
  }
}
