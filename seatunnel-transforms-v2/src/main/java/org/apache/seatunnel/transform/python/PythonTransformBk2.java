/*
package org.apache.seatunnel.transform.python;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.format.json.JsonToRowConverters;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.transform.common.HttpClientUtil;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;



*/
/**
 * @description:
 * @author: wangjing
 * @createDate: 2024/6/20
 *//*

@Slf4j
@Deprecated
public class PythonTransformBk2 extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "PythonScript";

    public static final Option<String> PYTHON_SCRIPT_FILE_ID =
            Options.key("python_script_file_id").stringType().noDefaultValue().withDescription("The Python script file id");

    private static final String DOWNLOAD_URL = "bp-file-server.download.url";

    private final String pythonScriptFileId;

    private final SeaTunnelRowType inputSeaTunnelRowType;

    private SeaTunnelRowType outputSeaTunnelRowType;

    private List<FieldConfig> fieldConfigs;

    private static final String PATH_PYTHON_BIN = "/usr/bin/python3";
    //    private static final String PATH_PYTHON_BIN = "C:\\software\\Python3\\python.exe";
    private final ObjectMapper objectMapper = new ObjectMapper();

    private String pythonScriptContent;


    */
/*    private PrintWriter writer;
        private BufferedReader reader;*//*

    private Process pythonProcess;
    private File tempScriptFile;


    private JsonToRowConverters.JsonToObjectConverter[] converters;

    public PythonTransformBk2(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        log.info("s0 start init config:{}",config);
        this.pythonScriptFileId = config.get(PYTHON_SCRIPT_FILE_ID);
        this.inputSeaTunnelRowType = catalogTable.getSeaTunnelRowType();
        this.fieldConfigs = PythonScriptTransformConfig.of(config).getFieldConfigs();
        log.info("s0 field configs:{}",JsonUtils.toJsonString(fieldConfigs));
        initOutputSeaTunnelRowType();
        log.info("s0 output seatunnelrowtype:{}",outputSeaTunnelRowType.toString());
        init();
    }

    private void initOutputSeaTunnelRowType() {
        SeaTunnelDataType<?>[] dataTypes =
                this.fieldConfigs.stream()
                        .map(FieldConfig::getOutputDataType)
                        .toArray(SeaTunnelDataType<?>[]::new);
        this.outputSeaTunnelRowType =
                new SeaTunnelRowType(
                        this.fieldConfigs.stream()
                                .map(FieldConfig::getName)
                                .toArray(String[]::new),
                        dataTypes);
    }

    private void init() {
        log.info("s0 init param pythonScriptField={}", pythonScriptFileId);
        if (StringUtils.isEmpty(pythonScriptFileId)) {
            log.error("[PythonTransform] python_script_file_id is must be not null");
            throw TransformCommonError.initTransformError(PythonTransformBk2.PLUGIN_NAME, null);
        }
        // get python script file
        this.pythonScriptContent = getFileContent();
        log.info("s2 file content{}", pythonScriptContent);
        JsonToRowConverters jsonToRowConverters = new JsonToRowConverters(false, false);
        this.converters = Arrays.stream(outputSeaTunnelRowType.getFieldTypes())
                .map(jsonToRowConverters::createConverter)
                .toArray(JsonToRowConverters.JsonToObjectConverter[]::new);

    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {

        int size = inputRow.getArity();
        List<Object> inputValues = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Object inputValueByType = inputRow.getField(i);
            inputValues.add(inputValueByType);
        }
        JsonNode jsonNode = exexcutePython(inputValues);
        int outSize = outputSeaTunnelRowType.getTotalFields();
        Object[] fieldValues = new Object[outSize];
        String[] fieldNames = outputSeaTunnelRowType.getFieldNames();
        for (int i = 0; i < outSize; i++) {
            String fieldName = fieldNames[i];
            fieldValues[i] = converters[i].convert(jsonNode.get(i), fieldName);
        }
        log.info("s5 fieldValues={}",Arrays.toString(fieldValues));
        return fieldValues;

    }

    @Override
    protected TableSchema transformTableSchema() {
        List<String> outputColumns = Arrays.asList(inputSeaTunnelRowType.getFieldNames());
        TableSchema.Builder builder = TableSchema.builder();
        if (inputCatalogTable.getTableSchema().getPrimaryKey() != null
                && new HashSet<>(outputColumns).containsAll(
                inputCatalogTable.getTableSchema().getPrimaryKey().getColumnNames())) {
            builder.primaryKey(inputCatalogTable.getTableSchema().getPrimaryKey().copy());
        }
        int size = this.fieldConfigs.size();
        List<Column> columns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            FieldConfig field = fieldConfigs.get(i);
            Column column = PhysicalColumn.of(field.getName(), field.getOutputDataType(), (Long)null, field.getNullable(),
                    field.getDefaultValue(), field.getComment());
            columns.add(column);
        }
        log.info("s6 output columns:{}", JsonUtils.toJsonString(columns));
        return builder.columns(columns).build();
    }

    @Override
    protected Column[] getOutputColumns() {
        return null;
    }

//    @Override
//    protected Column[] getOutputColumns() {
////        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();
////        return columns.toArray(new Column[0]);
//        int len = this.fieldConfigs.size();
//        Column[] columns = new Column[len];
//        for (int i = 0; i < len; i++) {
//            FieldConfig field = fieldConfigs.get(i);
//            columns[i] = PhysicalColumn.of(field.getName(), field.getOutputDataType(), (Long)null, field.getNullable(), field.getDefaultValue(), field.getComment());
//        }
//        return columns;
//    }

    */
/**
     * Get fileContent by url
     *
     * @return
     *//*

    private String getFileContent() {
        String configOption = Options.key(PYTHON_SCRIPT_FILE_ID.key()).stringType()
                .defaultValue(pythonScriptFileId).toString();
        try {
            Properties prop = new Properties();
            InputStream input = PythonTransformBk2.class.getClassLoader().getResourceAsStream("config.properties");
            prop.load(input);

            String url = prop.getProperty(DOWNLOAD_URL);
            log.info("s1 url={}", url);
            if (StringUtils.isEmpty(url)) {
                log.error("[PythonTransform]get download file url is null");
                throw TransformCommonError.initTransformError(PythonTransformBk2.PLUGIN_NAME, configOption);
            }
            String finalUrl = url.replace("${id}", pythonScriptFileId);
            log.info("s1 finalurl={}", finalUrl);
            //send http request
            String response = HttpClientUtil.sendGetRequest(finalUrl);
            if (StringUtils.isEmpty(response)) {
                log.error("[PythonTransform]can not find python script content by bp-file-server,url={}", finalUrl);
                throw TransformCommonError.initTransformError(PythonTransformBk2.PLUGIN_NAME, configOption);
            }
            return response;
        } catch (IOException e) {
            log.error("[PythonTransform]PythonTransform init error", e);
            throw TransformCommonError.initTransformError(PythonTransformBk2.PLUGIN_NAME, configOption);
        }
    }

    */
/**
     * execute python script
     *
     * @param inputValue
     * @return
     *//*

    public JsonNode exexcutePython(List<Object> inputValue) {
        try {

            //First method: file stream to python process
            ProcessBuilder processBuilder = new ProcessBuilder(PATH_PYTHON_BIN, "-c", pythonScriptContent);
            //Second method: create temp file
*/
/*            this.tempScriptFile = File.createTempFile("process", ".py");
            try (BufferedWriter bufferedWriter = Files.newBufferedWriter(tempScriptFile.toPath())) {
                bufferedWriter.write(pythonScriptContent);
            }
            ProcessBuilder processBuilder = new ProcessBuilder(PATH_PYTHON_BIN, tempScriptFile.getAbsolutePath());*//*

            processBuilder.redirectErrorStream(true);
            pythonProcess = processBuilder.start();

            try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(pythonProcess.getOutputStream()), true);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()))) {

                //Convert to json array as Python input
                String jsonInput = objectMapper.writeValueAsString(inputValue);
                log.info("s3 python request:{}", jsonInput);
                writer.println(jsonInput);
                writer.close();

                // Read output from Python process
                StringBuilder output = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line);
                }
                // Wait for Python process to complete
                int exitCode = pythonProcess.waitFor();
                log.info("Python process exited with code={}", exitCode);
                if (exitCode != 0) {
                    //python script error
                    log.error("[PythonTransform] python script file execute error,python process exitCode:{},python output:{}", exitCode, output);
                    throw TransformCommonError.pythonScriptFileError(output.toString());
                }

                JsonNode jsonNode = objectMapper.readTree(output.toString());
                log.info("s4 python response:{}", jsonNode.toString());
                return jsonNode;
            } catch (IOException | InterruptedException e) {
                log.error("[PythonTransform] transform by inputRow error,python_script_file_id={}", pythonScriptFileId, e);
                throw TransformCommonError.executeTransformError(PythonTransformBk2.PLUGIN_NAME, inputValue.toString());
            }
        } catch (IOException e) {
            log.error("[PythonTransform] Error starting Python process, python_script_file_id={}", pythonScriptFileId, e);
            throw new RuntimeException("Error starting Python process", e);
        } finally {
            if (pythonProcess != null) {
                pythonProcess.destroy();
            }
        }
    }
}

*/
