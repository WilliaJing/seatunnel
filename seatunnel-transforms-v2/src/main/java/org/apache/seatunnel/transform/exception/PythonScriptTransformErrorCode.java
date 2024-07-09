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
 */
package org.apache.seatunnel.transform.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum PythonScriptTransformErrorCode implements SeaTunnelErrorCode {
   FIELDS_MUST_NOT_EMPTY(
            "FIELDS_MUST_NOT_EMPTY", "【PythonScript transform】fields must not empty"),
    NAME_MUST_NOT_EMPTY(
            "NAME_MUST_NOT_EMPTY", "【PythonScript transform】name must not empty"),
    OUTPUT_DATA_TYPE_MUST_NOT_EMPTY(
            "OUTPUT_DATA_TYPE_MUST_NOT_EMPTY", "【PythonScript transform】outputDataType must not empty"),


    ;
    private final String code;
    private final String description;

    PythonScriptTransformErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
