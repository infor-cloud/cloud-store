/*
  Copyright 2017, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox.s3lib;

public class ProgressOptionsBuilder {
    private String objectUri;
    private String operation;
    private long fileSizeInBytes;

    public ProgressOptionsBuilder setObjectUri(String objectUri) {
        this.objectUri = objectUri;
        return this;
    }

    public ProgressOptionsBuilder setOperation(String operation) {
        this.operation = operation;
        return this;
    }

    public ProgressOptionsBuilder setFileSizeInBytes(long fileSizeInBytes) {
        this.fileSizeInBytes = fileSizeInBytes;
        return this;
    }

    public ProgressOptions createProgressOptions() {
        return new ProgressOptions(objectUri, operation, fileSizeInBytes);
    }
}
