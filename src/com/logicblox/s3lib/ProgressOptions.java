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

public class ProgressOptions {
    private final String objectUri;
    private final String operation;
    private final long fileSizeInBytes;

    ProgressOptions(String objectUri, String operation, long fileSizeInBytes) {
        this.objectUri = objectUri;
        this.operation = operation;
        this.fileSizeInBytes = fileSizeInBytes;
    }

    public String getObjectUri() {
        return objectUri;
    }

    public String getOperation() {
        return operation;
    }

    public long getFileSizeInBytes() {
        return fileSizeInBytes;
    }
}
