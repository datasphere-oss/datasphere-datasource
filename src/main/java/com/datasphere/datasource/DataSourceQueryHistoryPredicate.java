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

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Predicate;

import org.joda.time.DateTime;

public class DataSourceQueryHistoryPredicate {

  /**
   * Defining Conditions Related to Data Source Basic Search
   *
   * @param queryType type of query
   * @param succeed whether succeed
   * @param from Search start date, yyyy-MM-ddThh:mm:ss.SSSZ
   * @param to Search end date, yyyy-MM-ddThh:mm:ss.SSSZ
   * @return
   */
  public static Predicate searchList(String dataSourceId,
                                     DataSourceQueryHistory.QueryType queryType, Boolean succeed,
                                     DateTime from, DateTime to) {

    BooleanBuilder builder = new BooleanBuilder();
    QDataSourceQueryHistory queryHistory = QDataSourceQueryHistory.dataSourceQueryHistory;

    if(dataSourceId != null) {
      builder = builder.and(queryHistory.dataSourceId.eq(dataSourceId));
    }

    if(queryType != null) {
      builder = builder.and(queryHistory.queryType.eq(queryType));
    }

    if(succeed != null) {
      builder = builder.and(queryHistory.succeed.eq(succeed));
    }

    if(from != null && to != null) {
      builder = builder.and(queryHistory.modifiedTime.between(from, to));
    }

    return builder;
  }

}
