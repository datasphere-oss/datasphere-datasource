/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specic language governing permissions and
 * limitations under the License.
 */

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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.rest.core.annotation.HandleAfterCreate;
import org.springframework.data.rest.core.annotation.HandleAfterDelete;
import org.springframework.data.rest.core.annotation.HandleAfterSave;
import org.springframework.data.rest.core.annotation.HandleBeforeCreate;
import org.springframework.data.rest.core.annotation.HandleBeforeDelete;
import org.springframework.data.rest.core.annotation.HandleBeforeLinkDelete;
import org.springframework.data.rest.core.annotation.HandleBeforeLinkSave;
import org.springframework.data.rest.core.annotation.HandleBeforeSave;
import org.springframework.data.rest.core.annotation.RepositoryEventHandler;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;

import static com.datasphere.datasource.DataSource.ConnectionType.ENGINE;
import static com.datasphere.datasource.DataSource.ConnectionType.LINK;
import static com.datasphere.datasource.DataSource.SourceType.IMPORT;
import static com.datasphere.datasource.DataSource.SourceType.JDBC;
import static com.datasphere.datasource.DataSource.SourceType.NONE;
import static com.datasphere.datasource.DataSource.Status.ENABLED;
import static com.datasphere.datasource.DataSource.Status.FAILED;
import static com.datasphere.datasource.DataSource.Status.PREPARING;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.PostConstruct;

import com.datasphere.datasource.dataconnection.DataConnection;
import com.datasphere.datasource.dataconnection.DataConnectionRepository;
import com.datasphere.datasource.ingestion.HiveIngestionInfo;
import com.datasphere.datasource.ingestion.IngestionHistory;
import com.datasphere.datasource.ingestion.IngestionHistoryRepository;
import com.datasphere.datasource.ingestion.IngestionInfo;
import com.datasphere.datasource.ingestion.RealtimeIngestionInfo;
import com.datasphere.datasource.ingestion.jdbc.BatchIngestionInfo;
import com.datasphere.datasource.ingestion.jdbc.JdbcIngestionInfo;
import com.datasphere.datasource.ingestion.jdbc.LinkIngestionInfo;
import com.datasphere.datasource.ingestion.job.IngestionJobRunner;
import com.datasphere.datasource.service.DataSourceService;
import com.datasphere.server.common.domain.context.ContextService;
import com.datasphere.server.domain.activities.ActivityStreamService;
import com.datasphere.server.domain.activities.spec.ActivityGenerator;
import com.datasphere.server.domain.activities.spec.ActivityObject;
import com.datasphere.server.domain.activities.spec.ActivityStreamV2;
import com.datasphere.server.domain.engine.DruidEngineMetaRepository;
import com.datasphere.server.domain.engine.EngineIngestionService;
//import com.datasphere.server.domain.mdm.Metadata;
//import com.datasphere.server.domain.mdm.MetadataService;
//import com.datasphere.server.domain.workspace.Workspace;
import com.datasphere.server.util.AuthUtils;
import com.datasphere.server.util.PolarisUtils;

/**
 * Created by aladin on 2019. 4. 1..
 */
@RepositoryEventHandler(DataSource.class)
public class DataSourceEventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceEventHandler.class);

  @Autowired
  IngestionHistoryRepository ingestionHistoryRepository;

  @Autowired
  DruidEngineMetaRepository engineMetaRepository;

  @Autowired
  DataSourceService dataSourceService;

//  @Autowired
//  MetadataService metadataService;

  @Autowired
  DataSourceRepository dataSourceRepository;

  @Autowired
  DataConnectionRepository dataConnectionRepository;

  @Autowired
  EngineIngestionService engineIngestionService;

  @Autowired
  IngestionJobRunner jobRunner;

  @Autowired
  ContextService contextService;

  @Autowired
  DataSourceSizeHistoryRepository dataSourceSizeHistoryRepository;

  @Autowired
  DataSourceQueryHistoryRepository dataSourceQueryHistoryRepository;

  @Autowired
  ActivityStreamService activityStreamService;

  @Autowired(required = false)
  Scheduler scheduler;


  @HandleBeforeCreate
  @PreAuthorize("hasAuthority('PERM_SYSTEM_MANAGE_DATASOURCE') or " +
      "hasPermission(#dataSource, 'PERM_SYSTEM_MANAGE_DATASOURCE')")
  public void checkCreateAuthority(DataSource dataSource) {

    // Owner Enrollment
    if (StringUtils.isEmpty(dataSource.getOwnerId())) {
      dataSource.setOwnerId(AuthUtils.getAuthUserName());
    }

    IngestionInfo ingestionInfo = dataSource.getIngestionInfo();
    if (ingestionInfo instanceof JdbcIngestionInfo) {
      DataConnection dataConnection = Preconditions.checkNotNull(dataSource.getConnection() == null ?
                                                                     ((JdbcIngestionInfo) ingestionInfo).getConnection()
                                                                     : dataSource.getConnection());
      //Batch Ingestion not allow Dialog type connection
      if (ingestionInfo instanceof BatchIngestionInfo) {
        Preconditions.checkArgument(
            dataConnection.getAuthenticationType() != DataConnection.AuthenticationType.DIALOG,
            "BatchIngestion not allowed DIALOG Authentication.");
      }

      //Dialog Authentication require connectionUsername, connectionPassword
      if (ingestionInfo instanceof JdbcIngestionInfo
          && dataConnection.getAuthenticationType() == DataConnection.AuthenticationType.DIALOG) {

        Preconditions.checkNotNull(((JdbcIngestionInfo) ingestionInfo).getConnectionUsername(),
                                   "Dialog Authentication require connectionUsername.");

        Preconditions.checkNotNull(((JdbcIngestionInfo) ingestionInfo).getConnectionPassword(),
                                   "Dialog Authentication require connectionPassword.");
      }
    }

    //partition range to map
    if (ingestionInfo instanceof HiveIngestionInfo) {
      List<String> partitionNameList = new ArrayList<>();
      for (Map<String, Object> partitionNameMap : ((HiveIngestionInfo) ingestionInfo).getPartitions()) {
        partitionNameList.addAll(PolarisUtils.mapWithRangeExpressionToList(partitionNameMap));
      }

      List<Map<String, Object>> partitionMapList = new ArrayList<>();
      for (String partitionName : partitionNameList) {
        partitionMapList.add(PolarisUtils.partitionStringToMap(partitionName));
      }
      ((HiveIngestionInfo) ingestionInfo).setPartitions(partitionMapList);
      dataSource.setIngestionInfo(ingestionInfo);
    }

    /*
      Data loading related
     */
    if (dataSource.getConnType() == ENGINE) {

      if (dataSource.getSrcType() == NONE || dataSource.getSrcType() == IMPORT) {
        if (StringUtils.isNotEmpty(dataSource.getEngineName())) {
          dataSource.setEngineName(dataSource.getName());
        }
      } else {
        // Added to prevent datasource name collisions in the engine
        dataSource.setEngineName(dataSourceService.convertName(dataSource.getName()));
        dataSource.setStatus(PREPARING);
        dataSource.setIncludeGeo(dataSource.existGeoField()); // mark datasource include geo column
        dataSourceRepository.saveAndFlush(dataSource);

        if (dataSource.getIngestionInfo() instanceof RealtimeIngestionInfo) {
          Optional<IngestionHistory> ingestionHistory = engineIngestionService.realtimeIngestion(dataSource);

          IngestionHistory resultHistory = null;
          if (ingestionHistory.isPresent()) {
            resultHistory = ingestionHistory.get();
            if (resultHistory.getStatus() != IngestionHistory.IngestionStatus.FAILED) {
              dataSource.setStatus(ENABLED);
            } else {
              dataSource.setStatus(FAILED);
            }
          } else {
            resultHistory = new IngestionHistory(dataSource.getId(),
                                                 IngestionHistory.IngestionMethod.SUPERVISOR,
                                                 IngestionHistory.IngestionStatus.FAILED,
                                                 "Ingestion History not fond");
          }
          ingestionHistoryRepository.saveAndFlush(resultHistory);
        } else {
          ThreadFactory factory = new ThreadFactoryBuilder()
              .setNameFormat("ingestion-" + dataSource.getId() + "-%s")
              .setDaemon(true)
              .build();
          ExecutorService service = Executors.newSingleThreadExecutor(factory);
          service.submit(() -> jobRunner.ingestion(dataSource));
        }
      }

    } else if (dataSource.getConnType() == LINK) {

      // Added to prevent datasource name collisions in the engine
      dataSource.setEngineName(dataSourceService.convertName(dataSource.getName()));

      LinkIngestionInfo ingestion = (LinkIngestionInfo) dataSource.getIngestionInfo();

      Preconditions.checkNotNull(dataSource.getConnection() == null ?
                                     ingestion.getConnection() : dataSource.getConnection(),
                                 "Connection info. required");

      dataSource.setStatus(DataSource.Status.ENABLED);
    }
  }

  @HandleAfterCreate
  public void handleDataSourceAfterCreate(DataSource dataSource) {

    // Perform save if IngestionHistory exists
    IngestionHistory history = dataSource.getHistory();
    if (history != null) {
      history.setDataSourceId(dataSource.getId());
      ingestionHistoryRepository.save(history);
    }

    // save context from domain
    contextService.saveContextFromDomain(dataSource);

    if (dataSource.getConnType() == LINK) {
      // create metadata
      metadataService.saveFromDataSource(dataSource);
    }

    // Pass if not a collection path
    if (dataSource.getIngestion() == null) {
      return;
    }

    IngestionInfo info = dataSource.getIngestionInfo();
    if (scheduler != null && info instanceof BatchIngestionInfo) {
      // Verify Initial Ingestion Results
      JobKey jobKey = new JobKey("incremental-ingestion", "ingestion");
      TriggerKey triggerKey = new TriggerKey(dataSource.getId(), "ingestion");

      JobDataMap map = new JobDataMap();
      map.put("dataSourceId", dataSource.getId());

      // @formatter:of
      Trigger trigger = TriggerBuilder
          .newTrigger()
          .withIdentity(triggerKey)
          .forJob(jobKey)
          .usingJobData(map)
          .withSchedule(CronScheduleBuilder
                            .cronSchedule(((BatchIngestionInfo) info).getPeriod().getCronExpr())
                            .withMisfireHandlingInstructionDoNothing())
          .build();
      // @formatter:on

      try {
        scheduler.scheduleJob(trigger);
      } catch (SchedulerException e) {
        LOGGER.warn("Fail to register batch ingestion : {}", e.getMessage());
      }
      LOGGER.info("Successfully register batch ingestion : {}", dataSource.getName());
    }

  }

  @HandleBeforeSave
  @PreAuthorize("hasAuthority('PERM_SYSTEM_MANAGE_DATASOURCE')")
  public void handleBeforeSave(DataSource dataSource) {
    // Context Information storage
    contextService.saveContextFromDomain(dataSource);
  }

  @HandleBeforeLinkSave
  @PreAuthorize("hasAuthority('PERM_SYSTEM_MANAGE_DATASOURCE')")
  public void handleBeforeLinkSave(DataSource dataSource, Object linked) {

    // Count connected workspaces.
    // a value is injected to linked object when PATCH,
    // but not injected when PUT request so doesn't check linked object.
    if (BooleanUtils.isNotTrue(dataSource.getPublished())) {
      dataSource.setLinkedWorkspaces(dataSource.getWorkspaces().size());

      // Insert ActivityStream for saving grant history.
      if(!CollectionUtils.sizeIsEmpty(linked) && CollectionUtils.get(linked, 0) instanceof Workspace) {
        for (int i = 0; i < CollectionUtils.size(linked); i++) {
          Workspace linkedWorkspace = (Workspace) CollectionUtils.get(linked, i);
          if (!linkedWorkspace.getDataSources().contains(dataSource)) {
            activityStreamService.addActivity(new ActivityStreamV2(
                null, null, "Accept", null, null
                , new ActivityObject(dataSource.getId(),"DATASOURCE")
                , new ActivityObject(linkedWorkspace.getId(), "WORKSPACE"),
                new ActivityGenerator("WEBAPP",""), DateTime.now()));

            LOGGER.debug("[Activity] Accept workspace ({}) to datasource ({})", linkedWorkspace.getId(), dataSource.getId());
          }
        }
      }
    }
  }

  @HandleAfterSave
  public void handleDataSourceAfterSave(DataSource dataSource) {

    // Pass if not a batch collection path
    if (dataSource.getConnType() == ENGINE && dataSource.getIngestion() != null) {

      IngestionInfo ingestionInfo = dataSource.getIngestionInfo();

      if (ingestionInfo == null) {
        // Cancel after checking whether there is an existing loading task
        List<IngestionHistory> ingestionHistories = ingestionHistoryRepository
            .findByDataSourceIdAndStatus(dataSource.getId(), IngestionHistory.IngestionStatus.RUNNING);

        if (CollectionUtils.isNotEmpty(ingestionHistories)) {
          for (IngestionHistory ingestionHistory : ingestionHistories) {
            engineIngestionService.shutDownIngestionTask(ingestionHistory);
          }
        }

        // Terminate if existing Trigger is in operation
        if (scheduler != null) {
          TriggerKey triggerKey = new TriggerKey(dataSource.getId(), "ingestion");
          try {
            scheduler.unscheduleJob(triggerKey);
            LOGGER.info("Successfully unscheduled cron job for datasource({})", dataSource.getId());
          } catch (SchedulerException e) {
            LOGGER.warn("Fail to pause trigger : {} ", e.getMessage());
          }
        }

      } else if (ingestionInfo instanceof BatchIngestionInfo) {

        BatchIngestionInfo batchIngestionInfo = (BatchIngestionInfo) dataSource.getIngestionInfo();

        TriggerKey triggerKey = new TriggerKey(dataSource.getId(), "ingestion");
        CronTriggerImpl trigger;
        try {
          trigger = (CronTriggerImpl) scheduler.getTrigger(triggerKey);
        } catch (Exception e) {
          LOGGER.warn("Fail to get Trigger object : {} ", e.getMessage());
          return;
        }

        // Can be changed after comparing with existing Sheduling policy
        String cronExpr = trigger.getCronExpression();
        String afterCronExpr = batchIngestionInfo.getPeriod().getCronExpr();
        if (!cronExpr.equals(afterCronExpr)) {
          try {
            trigger.setCronExpression(afterCronExpr);
            scheduler.rescheduleJob(triggerKey, trigger);

            LOGGER.info("Successfully rescheduled cron job for datasource({}) : '{}' to '{}'",
                        dataSource.getId(), cronExpr, afterCronExpr);

          } catch (Exception e) {
            LOGGER.warn("Fail to get Trigger object : {} ", e.getMessage());
            return;
          }
        }
      } else if (ingestionInfo instanceof RealtimeIngestionInfo) {
        // TODO: Check if a loading task already exists (needs redefinition)
      }
    }

    // update metadata from datasource
    metadataService.updateFromDataSource(dataSource, false);
  }

//  @HandleBeforeDelete
//  @PreAuthorize("hasAuthority('PERM_SYSTEM_MANAGE_DATASOURCE')")
//  public void handleBeforeDelete(DataSource dataSource) {
//    // Context Delete info
//    contextService.removeContextFromDomain(dataSource);
//  }
//
//  @HandleBeforeLinkDelete
//  @PreAuthorize("hasAuthority('PERM_SYSTEM_MANAGE_DATASOURCE')")
//  public void handleBeforeLinkDelete(DataSource dataSource, Object linked) {
//    // Count connected workspaces.
//    Set<Workspace> preWorkspaces = dataSourceRepository.findWorkspacesInDataSource(dataSource.getId());
//    // Not a public workspace and linked entity type is Workspace.
//    if (BooleanUtils.isNotTrue(dataSource.getPublished()) &&
//        !CollectionUtils.sizeIsEmpty(preWorkspaces)) {
//      dataSource.setLinkedWorkspaces(dataSource.getWorkspaces().size());
//      LOGGER.debug("DELETED: Set linked workspace in datasource({}) : {}", dataSource.getId(), dataSource.getLinkedWorkspaces());
//
//      for (Workspace workspace : preWorkspaces) {
//        if(!dataSource.getWorkspaces().contains(workspace)) {
//          activityStreamService.addActivity(new ActivityStreamV2(
//              null, null, "Block", null, null,
//              new ActivityObject(dataSource.getId(), "DATASOURCE"),
//              new ActivityObject(workspace.getId(), "WORKSPACE"),
//              new ActivityGenerator("WEBAPP", ""), DateTime.now()));
//
//          LOGGER.debug("[Activity] Block workspace ({}) from datasource ({})", workspace.getId(), dataSource.getId());
//        }
//      }
//    }
//
//  }

  @HandleAfterDelete
  public void handleDataSourceAfterDelete(DataSource dataSource) {

    if (dataSource.getConnType() == ENGINE) {

      // Batch Loading Trigger End
      if (dataSource.getSrcType() == JDBC
          && dataSource.getIngestionInfo() instanceof BatchIngestionInfo) {

        TriggerKey triggerKey = new TriggerKey(dataSource.getId(), "ingestion");
        try {
          scheduler.unscheduleJob(triggerKey);
          LOGGER.info("Successfully unscheduled cron job for datasource({})", dataSource.getId());
        } catch (SchedulerException e) {
          LOGGER.warn("Fail to pause trigger : {} ", e.getMessage());
        }
      }

      // Shutdown Ingestion Task
      engineIngestionService.shutDownIngestionTask(dataSource.getId());
      LOGGER.debug("Successfully shutdown ingestion tasks in datasource ({})", dataSource.getId());

      // Disable DataSource
      try {
        engineMetaRepository.disableDataSource(dataSource.getEngineName());
        LOGGER.info("Successfully disabled datasource({})", dataSource.getId());
      } catch (Exception e) {
        LOGGER.warn("Fail to disable datasource({}) : {} ", dataSource.getId(), e.getMessage());
      }

      // Delete Related Histories
      try {
        // Delete Ingestion History
        ingestionHistoryRepository.deteleHistoryByDataSourceId(dataSource.getId());

        // Delete Size history and Query history
        dataSourceSizeHistoryRepository.deleteHistoryById(dataSource.getId());
        dataSourceQueryHistoryRepository.deleteQueryHistoryById(dataSource.getId());
      } catch (Exception e) {
        LOGGER.warn("Fail to remove history related datasource({}) : {} ", dataSource.getId(), e.getMessage());
      }

      // Delete Metadata
//      try{
//        Optional<Metadata> metadata = metadataService.findByDataSource(dataSource.getId());
//        if(metadata.isPresent()){
//          metadataService.delete(metadata.get().getId());
//        }
//      } catch (Exception e){
//        LOGGER.warn("Fail to remove metadata related datasource({}) : {} ", dataSource.getId(), e.getMessage());
//      }
    }

  }

  @PostConstruct
  void setGlobalSecurityContext() {
    SecurityContextHolder.setStrategyName(SecurityContextHolder.MODE_INHERITABLETHREADLOCAL);
  }

}
