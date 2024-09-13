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

import cn.hutool.db.nosql.mongo.MongoFactory;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

public class PythonTransformTest {

    @Test
    public void testTableSchemaSupport() {
        PythonTransformFactory pythonTransformFactory = new PythonTransformFactory();
        Assertions.assertNotNull(pythonTransformFactory.optionRule());
    }

    @Test
    public void test(){
        MongoDatabase mongo = MongoFactory.getDS("master").getMongo().getDatabase("data_platform");
        MongoCollection<Document> modelCollection = mongo.getCollection("b2a6a34bb2d44fefb447b6d27bfcd058");
        Document query = new Document();
        query.append("code","21");
        Document projection = new Document();
        projection.append("name",1);
        projection.append("_id",0);
        FindIterable<Document> documents = modelCollection.find(query).projection(projection);
        Object[] rs = new Object[1];
        Object o = documents.first().get("name");
        rs[0] = o;
        System.out.println(o);
        Object[] fieldValues = new Object[1];
        fieldValues[0]="test1";
        System.out.println(fieldValues.length);
        String[] output = new String[]{"abc"};

        String[] array = Arrays.stream(output)
                .collect(Collectors.toList())
                .toArray(new String[0]);
        System.out.println(array.length);
    }
}
