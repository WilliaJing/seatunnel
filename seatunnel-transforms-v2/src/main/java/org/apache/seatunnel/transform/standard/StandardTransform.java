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

package org.apache.seatunnel.transform.standard;

import cn.hutool.db.nosql.mongo.MongoFactory;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.exception.TransformException;
import org.bson.Document;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.seatunnel.transform.standard.StandardTransformErrorCode.STANDARD_TRANSFORM_ERROR_CODE;

@Slf4j
public class StandardTransform extends MultipleFieldOutputTransform {
    public static final String PLUGIN_NAME = "Standard";
    private final String[] queryModelField;
    private int[] inputIndex;
    private String outputFieldName;
    private String outputFieldType;
    private final String modelProjectionField;
    private final String modelId;
    private final String DEFAULT_DATABASE = "data_platform";
    private int outputIndex;
    private MongoCollection<Document> collection;

    public StandardTransform(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        super(catalogTable);
        SeaTunnelRowType physicalRowDataType = catalogTable.getTableSchema().toPhysicalRowDataType();
        modelId = readonlyConfig.get(StandardTransformConfig.MODEL_ID);
        List<ModelMappingRel> modelMappingRels = readonlyConfig.get(StandardTransformConfig.MODEL_MAPPING_REL);
        inputIndex = modelMappingRels.stream().mapToInt(rel -> physicalRowDataType.indexOf(rel.getInputField())).toArray();
        queryModelField = modelMappingRels.stream().map(ModelMappingRel::getQueryModelField).toArray(String[]::new);
        modelProjectionField = readonlyConfig.get(StandardTransformConfig.MODEL_PROJECTION_FIELD);
        outputFieldName = readonlyConfig.get(StandardTransformConfig.OUTPUT_FIELD_NAME);
        outputFieldType = readonlyConfig.get(StandardTransformConfig.OUTPUT_FIELD_TYPE);
        outputIndex = physicalRowDataType.indexOf(outputFieldName, false);
    }

    @Override
    public void open() {
        // 在这里初始化 MongoDB 连接
        if (collection == null) {
            collection = MongoFactory.getDS("master")
                    .getMongo()
                    .getDatabase(DEFAULT_DATABASE)
                    .getCollection(modelId);
        }
    }

    @Override
    public void close() {
        collection = null;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected Column[] getOutputColumns() {
        //新增/替换的字段构造Column返回即可
        SeaTunnelDataType<?> dataType =
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(outputFieldName, outputFieldType);
        PhysicalColumn destColumn =
                PhysicalColumn.of(
                        outputFieldName,
                        dataType,
                        20,
                        true,
                        "",
                        "");
        return new Column[]{
                destColumn
        };
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        if (collection == null) {
            open();
        }

        Document query = new Document();
        for (int i = 0; i < queryModelField.length; i++) {
            query.append(queryModelField[i], inputRow.getField(inputIndex[i]));
        }
        Document projection = new Document();
        projection.append("_id", 0);
        projection.append(modelProjectionField, 1);
        FindIterable<Document> documents = null;
        try {
            documents = collection.find(query).projection(projection);
        } catch (Exception e) {
            throw new TransformException(STANDARD_TRANSFORM_ERROR_CODE, e.getMessage());
        }
        Object rs = null;
        if (Objects.nonNull(documents.first())) {
            rs = documents.first().get(modelProjectionField);
        } else if (outputIndex > -1) {
            // 匹配不到映射的信息的情况：保持和原值相同
            rs = inputRow.getField(outputIndex);
        }
        return new Object[]{rs};
    }
}
