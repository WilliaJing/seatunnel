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

import lombok.Getter;
import lombok.Setter;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class StandardTransformConfig implements Serializable {

    public static final Option<String> MODEL_ID =
            Options.key("model_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The model id used for mapping");

    public static final Option<List<ModelMappingRel>> MODEL_MAPPING_REL =
            Options.key("model_mapping_rel")
                    .listType(ModelMappingRel.class)
                    .noDefaultValue()
                    .withDescription("Mapping between input fields and model fields");

    public static final Option<String> MODEL_PROJECTION_FIELD =
            Options.key("model_projection_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("model projection field");

//    public static final Option<MappingPattern> MAPPING_PATTERN =
//            Options.key("mapping_pattern")
//                    .enumType(MappingPattern.class)
//                    .defaultValue(MappingPattern.REPLACE)
//                    .withDescription("Post-mapping data processing mode");

    public static final Option<String> OUTPUT_FIELD_NAME =
            Options.key("output_field_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("replace or add field name");

    public static final Option<String> OUTPUT_FIELD_TYPE =
            Options.key("output_field_type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("replace or add field type");
}
