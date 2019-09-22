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

package com.datasphere.datasource.connections;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.JPAExpressions;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import java.util.List;

public class DataConnectionPredicate {

  /**
   * Defining Conditions Related to Data Source Basic Search
   *
   * @param namePattern DataConnection Characters contained within people
   * @param implementor DataConnection DB Type
   * @param searchDateBy Date search criteria (creation date / modification date)
   * @param from Search start date, yyyy-MM-ddThh:mm:ss.SSSZ
   * @param to Search end date, yyyy-MM-ddThh:mm:ss.SSSZ
   * @return
   */
  public static Predicate searchList(String namePattern,
                                     String implementor,
                                     String searchDateBy, DateTime from, DateTime to,
                                     DataConnection.AuthenticationType authenticationType) {

    QDataConnection dataConnection = QDataConnection.dataConnection;

    BooleanBuilder builder = new BooleanBuilder();

    if(implementor != null){
      builder.and(dataConnection.implementor.eq(implementor.toString()));
    }

    if(authenticationType != null){
      builder.and(dataConnection.authenticationType.eq(authenticationType));
    }

    if(StringUtils.isNotEmpty(namePattern)) {
      builder.and(dataConnection.name.containsIgnoreCase(namePattern));
    }

    if(from != null && to != null) {
      if(StringUtils.isNotEmpty(searchDateBy) && "CREATED".equalsIgnoreCase(searchDateBy)) {
        builder = builder.and(dataConnection.createdTime.between(from, to));
      } else {
        builder = builder.and(dataConnection.modifiedTime.between(from, to));
      }
    }

    return builder;
  }

  public static Predicate searchList(List<String> workspaces,
                                     List<String> createdBys,
                                     List<String> implementors,
                                     List<DataConnection.AuthenticationType> authenticationTypes,
                                     DateTime createdTimeFrom,
                                     DateTime createdTimeTo,
                                     DateTime modifiedTimeFrom,
                                     DateTime modifiedTimeTo,
                                     String containsText,
                                     List<Boolean> published) {

    QDataConnection dataConnection = QDataConnection.dataConnection;

    BooleanBuilder builder = new BooleanBuilder();

    //implementors
    if(implementors != null && !implementors.isEmpty()){
      BooleanBuilder subBuilder = new BooleanBuilder();
      for(String implementor : implementors){
        subBuilder = subBuilder.or(dataConnection.implementor.eq(implementor));
      }
      builder = builder.and(subBuilder);
    }

    //authenticationTypes
    if(authenticationTypes != null && !authenticationTypes.isEmpty()){
      BooleanBuilder subBuilder = new BooleanBuilder();
      for(DataConnection.AuthenticationType authenticationType : authenticationTypes){
        if(authenticationType == DataConnection.AuthenticationType.MANUAL){
          subBuilder = subBuilder.or(dataConnection.authenticationType.eq(authenticationType))
                  .or(dataConnection.authenticationType.isNull());
        } else {
          subBuilder = subBuilder.or(dataConnection.authenticationType.eq(authenticationType));
        }
      }
      builder = builder.and(subBuilder);
    }

    //CreatedBy
    if(createdBys != null && !createdBys.isEmpty()){
      BooleanBuilder subBuilder = new BooleanBuilder();
      for(String createdBy : createdBys){
        subBuilder = subBuilder.or(dataConnection.createdBy.eq(createdBy));
      }
      builder = builder.and(subBuilder);
    }

    //containsText
    if(StringUtils.isNotEmpty(containsText)){
      builder = builder.andAnyOf(dataConnection.name.containsIgnoreCase(containsText),
              dataConnection.description.containsIgnoreCase(containsText));
    }

    //createdTime
    if(createdTimeFrom != null && createdTimeTo != null) {
      builder = builder.and(dataConnection.createdTime.between(createdTimeFrom, createdTimeTo));
    } else if(createdTimeFrom != null){
      builder = builder.and(dataConnection.createdTime.goe(createdTimeFrom));
    } else if(createdTimeTo != null){
      builder = builder.and(dataConnection.createdTime.loe(createdTimeTo));
    }

    //modifiedTime
    if(modifiedTimeFrom != null && modifiedTimeTo != null) {
      builder = builder.and(dataConnection.modifiedTime.between(modifiedTimeFrom, modifiedTimeTo));
    } else if(modifiedTimeFrom != null){
      builder = builder.and(dataConnection.modifiedTime.goe(modifiedTimeFrom));
    } else if(modifiedTimeTo != null){
      builder = builder.and(dataConnection.modifiedTime.loe(modifiedTimeTo));
    }

    //published
    if(published != null && !published.isEmpty() && workspaces != null && !workspaces.isEmpty()){
      BooleanBuilder subBuilder = new BooleanBuilder();
      for(Boolean publishedBoolean : published){
        subBuilder = subBuilder.or(dataConnection.published.eq(publishedBoolean));
      }
      subBuilder = subBuilder.or(dataConnection.id.in(JPAExpressions.select(dataConnection.id)
              .from(dataConnection)
              .innerJoin(dataConnection.workspaces)
              .where(dataConnection.workspaces.any().id.in(workspaces))));
      builder.and(subBuilder);
    } else if(published != null && !published.isEmpty()){
      BooleanBuilder subBuilder = new BooleanBuilder();
      for(Boolean publishedBoolean : published){
        subBuilder = subBuilder.or(dataConnection.published.eq(publishedBoolean));
      }
      builder.and(subBuilder);
    } else if(workspaces != null && !workspaces.isEmpty()){
      BooleanExpression workspaceContains = dataConnection.id
              .in(JPAExpressions.select(dataConnection.id)
                      .from(dataConnection)
                      .innerJoin(dataConnection.workspaces)
                      .where(dataConnection.workspaces.any().id.in(workspaces)));
      builder.and(workspaceContains);
    }

    return builder;
  }

  /**
   *Defining Conditions Related to Data Source Basic Search
   *
   * @param namePattern DataConnection Characters contained within people
   * @param implementor DataConnection DB Type
   * @param workspaceId The inquiry criteria workspace
   * @return
   */
  public static Predicate searchListForWorkspace(String namePattern,
                                     String implementor,
                                     DataConnection.AuthenticationType authenticationType,
                                     String workspaceId) {

    QDataConnection dataConnection = QDataConnection.dataConnection;

    BooleanBuilder builder = (BooleanBuilder) searchList(namePattern, implementor,
            null, null, null, authenticationType);

    //For a specific workspace
    if(StringUtils.isNotEmpty(workspaceId)){
      //All open Connection OR Connection open only in the workspace
      builder = builder.and(dataConnection.published.isTrue()
              .or(dataConnection.workspaces.any().id.eq(workspaceId)));
    }

    return builder;
  }

}
