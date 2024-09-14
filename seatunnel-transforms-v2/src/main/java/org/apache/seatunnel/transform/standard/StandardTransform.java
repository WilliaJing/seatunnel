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

import java.util.Objects;

import static org.apache.seatunnel.transform.standard.StandardTransformErrorCode.STANDARD_TRANSFORM_ERROR_CODE;

@Slf4j
public class StandardTransform extends MultipleFieldOutputTransform {
    public static final String PLUGIN_NAME = "Standard";
    private final String[] queryModelFiled;
    private int[] inputIndex;
    private String outputFiledName;
    private String outputFiledType;
    private final String modelProjectionFiled;
    private final String modelId;
    private final String DEFAULT_DATABASE = "data_platform";

    public StandardTransform(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        super(catalogTable);
        SeaTunnelRowType physicalRowDataType = catalogTable.getTableSchema().toPhysicalRowDataType();
        modelId = readonlyConfig.get(StandardTransformConfig.MODEL_ID);
        String[] inputFiled = readonlyConfig.get(StandardTransformConfig.INPUT_FILED).split(",");
        inputIndex = new int[inputFiled.length];
        for (int i = 0; i < inputFiled.length; i++) {
            log.info("index:{}", physicalRowDataType.indexOf(inputFiled[i]));
            inputIndex[i] = physicalRowDataType.indexOf(inputFiled[i]);
        }
        queryModelFiled = readonlyConfig.get(StandardTransformConfig.QUERY_MODEL_FILED).split(",");
        modelProjectionFiled = readonlyConfig.get(StandardTransformConfig.MODEL_PROJECTION_FILED);
        outputFiledName = readonlyConfig.get(StandardTransformConfig.OUTPUT_FILED_NAME);
        outputFiledType = readonlyConfig.get(StandardTransformConfig.OUTPUT_FILED_TYPE);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected Column[] getOutputColumns() {
        //新增/替换的字段构造Column返回即可
        SeaTunnelDataType<?> dataType =
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(outputFiledName, outputFiledType);
        PhysicalColumn destColumn =
                PhysicalColumn.of(
                        outputFiledName,
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
        Document query = new Document();
        for (int i = 0; i < queryModelFiled.length; i++) {
            query.append(queryModelFiled[i], inputRow.getField(inputIndex[i]));
        }
        Document projection = new Document();
        projection.append("_id", 0);
        projection.append(modelProjectionFiled, 1);
        FindIterable<Document> documents = null;
        try {
            MongoCollection<Document> collection = MongoFactory.getDS("master").getMongo().getDatabase(DEFAULT_DATABASE).getCollection(modelId);
            documents = collection.find(query).projection(projection);
        } catch (Exception e) {
            throw new TransformException(STANDARD_TRANSFORM_ERROR_CODE, e.getMessage());
        }
        Object[] rs = new Object[1];
        rs[0] = (Objects.nonNull(documents.first())) ? documents.first().get(modelProjectionFiled) : null;
        log.info("Standard转换输出值:{}", rs[0]);
        return rs;

    }
}
