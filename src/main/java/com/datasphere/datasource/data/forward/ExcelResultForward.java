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

package com.datasphere.datasource.data.forward;

import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Created by aladin on 2019. 8. 24..
 */
@JsonTypeName("excel")
public class ExcelResultForward extends ResultForward {

  int maxRowsPerSheet = 1000000;

  public ExcelResultForward() {
  }

  public ExcelResultForward(int maxRowsPerSheet) {
    this.maxRowsPerSheet = maxRowsPerSheet;
  }

  public ExcelResultForward(String forwardUrl) {
    super(forwardUrl);
  }

  public int getMaxRowsPerSheet() {
    return maxRowsPerSheet;
  }

  public void setMaxRowsPerSheet(int maxRowsPerSheet) {
    this.maxRowsPerSheet = maxRowsPerSheet;
  }

  @Override
  public ForwardType getForwardType() {
    return ForwardType.EXCEL;
  }

}