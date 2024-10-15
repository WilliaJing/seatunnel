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

package org.apache.seatunnel.connectors.seatunnel.mongodb.serde;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.MongodbWriterOptions;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.conversions.Bson;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.serde.MongoDefaultField.*;

@Slf4j
public class RowDataDocumentSerializer implements DocumentSerializer<SeaTunnelRow> {

    private final RowDataToBsonConverters.RowDataToBsonConverter rowDataToBsonConverter;
    private final boolean isUpsertEnable;
    private final Function<BsonDocument, BsonDocument> filterConditions;

    private final Map<RowKind, WriteModelSupplier> writeModelSuppliers;

    public RowDataDocumentSerializer(
            RowDataToBsonConverters.RowDataToBsonConverter rowDataToBsonConverter,
            MongodbWriterOptions options,
            Function<BsonDocument, BsonDocument> filterConditions) {
        this.rowDataToBsonConverter = rowDataToBsonConverter;
        this.isUpsertEnable = options.isUpsertEnable();
        this.filterConditions = filterConditions;

        writeModelSuppliers = createWriteModelSuppliers();
    }

    public WriteModel<BsonDocument> serializeToWriteModel(SeaTunnelRow row) {
        WriteModelSupplier writeModelSupplier = writeModelSuppliers.get(row.getRowKind());
        if (writeModelSupplier == null) {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT, "Unsupported message kind: " + row.getRowKind());
        }
        return writeModelSupplier.get(row);
    }

    private Map<RowKind, WriteModelSupplier> createWriteModelSuppliers() {
        Map<RowKind, WriteModelSupplier> writeModelSuppliers = new HashMap<>();
        
        WriteModelSupplier upsertSupplier =
                row -> {
                    final BsonDocument bsonDocument = rowDataToBsonConverter.convert(row);
                    Bson filter = generateFilter(filterConditions.apply(bsonDocument));
                    bsonDocument.remove("_id");
                    bsonDocument.put(SYNC_UPDATE_TIME,new BsonDateTime(new Date().getTime()));
                    BsonDocument update = new BsonDocument("$set", bsonDocument);
                    //$set and $setOnInsert fields can not to be repeated
                    BsonDocument insertBsonDocument = new BsonDocument();
                    insertBsonDocument.put(SYNC_CREATE_TIME,new BsonDateTime(new Date().getTime()));
                    insertBsonDocument.put(SYNC_DELETED,new BsonInt32(0));
                    update.put("$setOnInsert",insertBsonDocument);
                    UpdateOneModel<BsonDocument> upsertOneModel= new UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));
                    log.info("upsertOneModel:{}",upsertOneModel);
                    return upsertOneModel;
                };
        WriteModelSupplier updateSupplier =
                row -> {
                    final BsonDocument bsonDocument = rowDataToBsonConverter.convert(row);
                    Bson filter = generateFilter(filterConditions.apply(bsonDocument));
                    bsonDocument.remove("_id");
                    bsonDocument.put(SYNC_UPDATE_TIME,new BsonDateTime(new Date().getTime()));
                    BsonDocument update = new BsonDocument("$set", bsonDocument);
                    UpdateOneModel<BsonDocument> updateOneModel = new UpdateOneModel<>(filter, update);
                    log.info("updateOneModel:{}",updateOneModel);
                    return updateOneModel;
                };
        WriteModelSupplier insertSupplier =
                row -> {
                    final BsonDocument bsonDocument = rowDataToBsonConverter.convert(row);
                    bsonDocument.put(SYNC_DELETED,new BsonInt32(0));
                    bsonDocument.put(SYNC_CREATE_TIME,new BsonDateTime(new Date().getTime()));
                    bsonDocument.put(SYNC_UPDATE_TIME,new BsonDateTime(new Date().getTime()));
                    InsertOneModel<BsonDocument> insertOneModel = new InsertOneModel<>(bsonDocument);
                    log.info("insertOneModel:{}",insertOneModel);
                    return insertOneModel;
                };
        WriteModelSupplier deleteSupplier =
                row -> {
                    final BsonDocument bsonDocument = rowDataToBsonConverter.convert(row);
                    bsonDocument.put(SYNC_DELETED,new BsonInt32(1));
                    bsonDocument.put(SYNC_UPDATE_TIME,new BsonDateTime(new Date().getTime()));
                    Bson filter = generateFilter(filterConditions.apply(bsonDocument));
                    DeleteOneModel<BsonDocument> deleteOneModel = new DeleteOneModel<>(filter);
                    log.info("deleteOneModel:{}",deleteOneModel);
                    return deleteOneModel;
                };
        writeModelSuppliers.put(RowKind.INSERT, isUpsertEnable ? upsertSupplier : insertSupplier);
        writeModelSuppliers.put(
                RowKind.UPDATE_AFTER, isUpsertEnable ? upsertSupplier : updateSupplier);
        writeModelSuppliers.put(RowKind.DELETE, deleteSupplier);
        return writeModelSuppliers;
    }

    public static Bson generateFilter(BsonDocument filterConditions) {
        List<Bson> filters =
                filterConditions.entrySet().stream()
                        .map(entry -> Filters.eq(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

        return Filters.and(filters);
    }

    private interface WriteModelSupplier {
        WriteModel<BsonDocument> get(SeaTunnelRow row);
    }
}
