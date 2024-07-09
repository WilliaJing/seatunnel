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
package org.apache.seatunnel.transform.python;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.transform.exception.PythonScriptTransformErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.seatunnel.transform.exception.PythonScriptTransformErrorCode.NAME_MUST_NOT_EMPTY;
import static org.apache.seatunnel.transform.exception.PythonScriptTransformErrorCode.OUTPUT_DATA_TYPE_MUST_NOT_EMPTY;

public class PythonScriptTransformConfig implements Serializable {

    public static final Option<String> NAME =
            Options.key("name")
                    .stringType()
                    .noDefaultValue();

    public static final Option<String> COMMENT =
            Options.key("comment")
                    .stringType()
                    .noDefaultValue();

    public static final Option<Boolean> PRIMARY_KEY =
            Options.key("primaryKey")
                    .booleanType()
                    .defaultValue(false);

    public static final Option<String> DEFAULT_VALUE =
            Options.key("defaultValue")
                    .stringType()
                    .noDefaultValue();
    public static final Option<Boolean> NULLABLE =
            Options.key("nullable")
                    .booleanType()
                    .defaultValue(true);
    public static final Option<String> OUTPUT_DATA_TYPE =
            Options.key("outputDataType")
                    .stringType()
                    .noDefaultValue();

    public static final Option<List<Map<String, String>>> FIELDS =
            Options.key("fields")
                    .type(new TypeReference<List<Map<String, String>>>() {})
                    .noDefaultValue()
                    .withDescription("fields");


    private final List<FieldConfig> fieldConfigs;

    public List<FieldConfig> getFieldConfigs(){
        return fieldConfigs;
    }

    public PythonScriptTransformConfig(List<FieldConfig> fieldConfigs) {
        this.fieldConfigs = fieldConfigs;
    }

    public static PythonScriptTransformConfig of(ReadonlyConfig config) {
        if (!config.toConfig().hasPath(FIELDS.key())) {
            throw new TransformException(PythonScriptTransformErrorCode.FIELDS_MUST_NOT_EMPTY,
                    PythonScriptTransformErrorCode.FIELDS_MUST_NOT_EMPTY.getErrorMessage());
        }
        List<Map<String, String>> fields = config.get(FIELDS);
        List<FieldConfig> configs = new ArrayList<>(fields.size());
        for (Map<String, String> map : fields) {
            configs.add(buildFieldConfig(map));
        }
        return new PythonScriptTransformConfig(configs);
    }

    private static FieldConfig buildFieldConfig(Map<String, String> map) {
        FieldConfig fieldConfig = new FieldConfig();
        String name = map.get(NAME.key());
        if (StringUtils.isBlank(name)) {
            throw new TransformException(
                    NAME_MUST_NOT_EMPTY, NAME_MUST_NOT_EMPTY.getErrorMessage());
        }
        fieldConfig.setName(name);

        String outputDataType = map.get(OUTPUT_DATA_TYPE.key());
        if (!Objects.nonNull(outputDataType)){
            throw new TransformException(
                    OUTPUT_DATA_TYPE_MUST_NOT_EMPTY, OUTPUT_DATA_TYPE_MUST_NOT_EMPTY.getErrorMessage());
        }
        SeaTunnelDataType<?> dataType =
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(name, outputDataType);
        fieldConfig.setOutputDataType(dataType);
        fieldConfig.setComment(map.get(COMMENT.key()));
        fieldConfig.setPrimaryKey(Boolean.parseBoolean(map.get(PRIMARY_KEY.key())));
        fieldConfig.setNullable(Boolean.parseBoolean(map.get(NULLABLE.key())));
        fieldConfig.setDefaultValue(map.get(DEFAULT_VALUE.key()));
        return fieldConfig;
    }
}
