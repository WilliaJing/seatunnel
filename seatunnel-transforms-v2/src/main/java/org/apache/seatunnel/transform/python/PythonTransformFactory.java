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

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import static org.apache.seatunnel.transform.python.PythonTransform.PYTHON_SCRIPT_FILE_ID;


@AutoService(Factory.class)
@Slf4j
public class PythonTransformFactory implements TableTransformFactory {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public String factoryIdentifier() {
        return PythonTransform.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(PYTHON_SCRIPT_FILE_ID).build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        try {
            log.info("init context:{}",objectMapper.writeValueAsString(context.getCatalogTables()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        CatalogTable catalogTable = context.getCatalogTables().get(0);
        return () -> new PythonTransform(context.getOptions(), catalogTable);
    }
}
