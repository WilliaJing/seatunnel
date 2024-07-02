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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
                    .defaultValue(false);
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
            throw TransformCommonError.pythonFieldError();
        }
        List<Map<String, String>> fields = config.get(FIELDS);
        List<FieldConfig> configs = new ArrayList<>(fields.size());
        for (Map<String, String> map : fields) {
            checkFieldConfig(map);
            System.out.println("....."+map.toString());
            FieldConfig fieldConfig = new FieldConfig();
            fieldConfig.setName(map.get(NAME.key()));
            fieldConfig.setComment(map.get(COMMENT.key()));
            fieldConfig.setPrimaryKey(Boolean.parseBoolean(map.get(PRIMARY_KEY.key())));
            fieldConfig.setNullable(Boolean.parseBoolean(map.get(NULLABLE.key())));
            fieldConfig.setDefaultValue(map.get(DEFAULT_VALUE.key()));
            String type = map.get(OUTPUT_DATA_TYPE.key());
            SeaTunnelDataType<?> dataType =
                    SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(fieldConfig.getName(), type);
            fieldConfig.setOutputDataType(dataType);
            configs.add(fieldConfig);
        }
        return new PythonScriptTransformConfig(configs);
    }

    private static void checkFieldConfig(Map<String, String> map) {
        //todo: fields校验

    }
}
