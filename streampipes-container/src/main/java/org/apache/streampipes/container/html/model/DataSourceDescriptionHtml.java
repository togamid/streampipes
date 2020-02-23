/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.streampipes.container.html.model;

import org.apache.streampipes.model.container.PeContainerElementDescription;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class DataSourceDescriptionHtml extends PeContainerElementDescription {

    private List<PeContainerElementDescription> streams;

    public DataSourceDescriptionHtml(String name, String description, URI uri, List<PeContainerElementDescription> streams) {
        super(name, description, uri);
        this.streams = streams;
    }

    public DataSourceDescriptionHtml() {
        streams = new ArrayList<>();
    }

    public List<PeContainerElementDescription> getStreams() {
        return streams;
    }

    public void setStreams(List<PeContainerElementDescription> streams) {
        this.streams = streams;
    }
}
