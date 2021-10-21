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

package org.apache.streampipes.monitoring;

public class AdapterStatus {
    private long timestamp;
    private long lastEvent;
    private int count;

    public AdapterStatus() {
        reset();
    }

    public void reset() {
        timestamp = -1;
        count = 0;
    }

    public void increaseCount(Long newTimestamp) {
        this.lastEvent = newTimestamp;
        this.count++;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getCount() {
        return count;
    }

    public long getLastEvent() {
        return lastEvent;
    }
}
