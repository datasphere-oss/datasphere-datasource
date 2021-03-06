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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specic language governing permissions and
 * limitations under the License.
 */

package com.datasphere.datasource.ingestion.job;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.datasource.DataSource;
import com.datasphere.datasource.DataSourceErrorCodes;
import com.datasphere.datasource.DataSourceIngestionException;
import com.datasphere.datasource.ingestion.IngestionHistory;
import com.datasphere.datasource.ingestion.IngestionHistoryRepository;
import com.datasphere.datasource.ingestion.IngestionOptionService;
import com.datasphere.server.common.GlobalObjectMapper;
import com.datasphere.server.common.fileloader.FileLoaderFactory;
import com.datasphere.server.common.fileloader.FileLoaderProperties;
import com.datasphere.server.domain.engine.DruidEngineMetaRepository;
import com.datasphere.server.domain.engine.DruidEngineRepository;
import com.datasphere.server.domain.engine.EngineProperties;
import com.datasphere.server.domain.storage.StorageProperties;
import com.datasphere.server.spec.druid.ingestion.Index;
import com.fasterxml.jackson.core.JsonProcessingException;

public abstract class AbstractIngestionJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIngestionJob.class);

  protected EngineProperties engineProperties;

  protected FileLoaderFactory fileLoaderFactory;

  protected IngestionHistoryRepository historyRepository;

  protected DruidEngineMetaRepository engineMetaRepository;

  protected DruidEngineRepository engineRepository;

  protected IngestionOptionService ingestionOptionService;

  protected DataSource dataSource;

  protected IngestionHistory ingestionHistory;

  protected String dedicatedWorker;

  protected List<String> loadedFilePaths;

  protected boolean noDateTimeField;

  protected StorageProperties storageProperties;

  public AbstractIngestionJob(DataSource dataSource, IngestionHistory ingestionHistory) {
    this.dataSource = dataSource;
    this.ingestionHistory = ingestionHistory;
  }

  public void setEngineProperties(EngineProperties engineProperties) {
    this.engineProperties = engineProperties;
  }

  public void setFileLoaderFactory(FileLoaderFactory fileLoaderFactory) {
    this.fileLoaderFactory = fileLoaderFactory;
  }

  public void setHistoryRepository(IngestionHistoryRepository historyRepository) {
    this.historyRepository = historyRepository;
  }

  public void setEngineMetaRepository(DruidEngineMetaRepository engineMetaRepository) {
    this.engineMetaRepository = engineMetaRepository;
  }

  public void setEngineRepository(DruidEngineRepository engineRepository) {
    this.engineRepository = engineRepository;
  }

  public void setIngestionOptionService(IngestionOptionService ingestionOptionService) {
    this.ingestionOptionService = ingestionOptionService;
  }

  protected void setDedicatedWoker() {
    if(StringUtils.isNotEmpty(dedicatedWorker)) {
      return;
    }

    dedicatedWorker = engineMetaRepository.dedicatedWorker().orElse(null);
  }

  public void setStorageProperties(StorageProperties storageProperties) {
    this.storageProperties = storageProperties;
  }

  /**
   * load file to engine(Ingestion Worker)
   *
   * @return Dedicated worker host, if success.
 * @throws DataSourceIngestionException 
   */
  protected void loadFileToEngine(List<String> fileNames, List<String> targetNames)  {

    setDedicatedWoker();

    String targetHost = StringUtils.substringBefore(dedicatedWorker, ":");

    EngineProperties.IngestionInfo ingestionInfo = engineProperties.getIngestion();
    FileLoaderProperties loaderProperties = ingestionInfo.getLoader();
    try {
      loadedFilePaths = fileLoaderFactory.put(targetHost, loaderProperties, fileNames, targetNames, true);
    } catch (Exception e) {
      // Fail to load file to engine(middle manager)
      try {
		throw new DataSourceIngestionException(DataSourceErrorCodes.INGESTION_FILE_LOAD_ERROR, "Failed to load the file into the engine", e);
	} catch (DataSourceIngestionException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
    }
  }

  /**
   * Send Ingestion Task to engine(overlord) by Spec.
 * @throws DataSourceIngestionException 
   */
  protected String doIngestion(Index spec) throws DataSourceIngestionException {

    String ingestionSpec = null;
    try {
      ingestionSpec = GlobalObjectMapper.getDefaultMapper().writeValueAsString(spec);
    } catch (JsonProcessingException e) {/* Empty statement */}

    LOGGER.info("Ingestion Spec for {} : {}", dataSource.getId(), ingestionSpec);

    String result;
    try {
      result = engineRepository.ingestion(ingestionSpec, String.class)
                               .orElseThrow(() -> new DataSourceIngestionException(DataSourceErrorCodes.INGESTION_ENGINE_TASK_CREATION_ERROR, "No response from the engine."));
    } catch (Exception e) {
      LOGGER.error("Failed to access to the engine", e);
      throw new DataSourceIngestionException(DataSourceErrorCodes.INGESTION_ENGINE_ACCESS_ERROR, "Failed to access to the engine", e);
    }

    LOGGER.info("Ingestion Result for {} : {} ", dataSource.getId(), result);


    String taskId;
    try {
      Map<String, Object> response = GlobalObjectMapper.getDefaultMapper().readValue(result, Map.class);
      if (response.containsKey("task")) {
        taskId = (String) response.get("task");
      } else {
        throw new IllegalArgumentException("Task Id not found.");
      }
    } catch (Exception e) {
      LOGGER.error("Fail to parse ingestion result", e);
      throw new DataSourceIngestionException(DataSourceErrorCodes.INGESTION_ENGINE_TASK_CREATION_ERROR, "Fail to parse ingestion result");
    }

    return taskId;
  }

}
