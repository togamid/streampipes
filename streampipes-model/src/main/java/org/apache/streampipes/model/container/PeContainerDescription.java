/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.streampipes.model.container;

import java.util.List;

public class PeContainerDescription {

  private PeContainerApiInfo apiInfo;
  private List<PeContainerElementDescription> elementDescriptions;

  public PeContainerDescription(PeContainerApiInfo apiInfo, List<PeContainerElementDescription> elementDescriptions) {
    this.apiInfo = apiInfo;
    this.elementDescriptions = elementDescriptions;
  }

  public PeContainerApiInfo getApiInfo() {
    return apiInfo;
  }

  public void setApiInfo(PeContainerApiInfo apiInfo) {
    this.apiInfo = apiInfo;
  }

  public List<PeContainerElementDescription> getElementDescriptions() {
    return elementDescriptions;
  }

  public void setElementDescriptions(List<PeContainerElementDescription> elementDescriptions) {
    this.elementDescriptions = elementDescriptions;
  }
}
