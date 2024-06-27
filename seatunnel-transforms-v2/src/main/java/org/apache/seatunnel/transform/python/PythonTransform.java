package org.apache.seatunnel.transform.python;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.JsonPathException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.format.json.JsonToRowConverters;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;
import org.apache.seatunnel.transform.common.HttpClientUtil;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.jsonpath.ColumnConfig;
import org.apache.seatunnel.transform.python.PythonEngineFactory.PythonEngineType;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.seatunnel.transform.exception.JsonPathTransformErrorCode.JSON_PATH_COMPILE_ERROR;


/**
 * @description:
 * @author: wangjing
 * @createDate: 2024/6/20
 */
@Slf4j
public class PythonTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "PythonScript";

    public static final Option<String> PYTHON_SCRIPT_FILE_ID =
            Options.key("python_script_file_id").stringType().noDefaultValue().withDescription("The Python script file id");

    private static final String DOWNLOAD_URL = "bp-file-server.download.url";

    private final String pythonScriptFileId;

    private final SeaTunnelRowType seaTunnelRowType;

    private static final String PATH_PYTHON_BIN = "/usr/bin/python3";
    //    private static final String PATH_PYTHON_BIN = "C:\\software\\Python3\\python.exe";
    private final ObjectMapper objectMapper = new ObjectMapper();

    private String pythonScriptContent;


    private PrintWriter writer;
    private BufferedReader reader;
    private Process pythonProcess;
    private File tempScriptFile;

    public PythonTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.pythonScriptFileId = config.get(PYTHON_SCRIPT_FILE_ID);
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        //通过文件id获取文件流
        init();
    }

    public void init() {
        if (StringUtils.isEmpty(pythonScriptFileId)) {
            log.error("[PythonTransform]python_script_file_id is must be not null");
            throw TransformCommonError.initTransformError(PythonTransform.PLUGIN_NAME, null);
        }
        // get python script file
        this.pythonScriptContent = getFileContent();
        log.info("PythonScriptFileContent={}", pythonScriptContent);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void close() {
        if (null != writer) {
            writer.close();
        }
        if (null != reader) {
            try {
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException("Error closing Python process", e);
            }
        }

        if (null != pythonProcess) {
            pythonProcess.destroy();
        }
/*        if(null != tempScriptFile) {
            tempScriptFile.deleteOnExit();
        }*/
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {

        int size = inputRow.getArity();
        Object[] fieldValues = new Object[size];
        List<Object> inputValues = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Object inputValueByType = transformValueByType(seaTunnelRowType.getFieldNames()[i], seaTunnelRowType.getFieldType(i), inputRow.getField(i));
            inputValues.add(inputValueByType);
        }
        log.info("Input python script transform:{}", inputValues);
        JsonNode jsonNode = exexcutePython(inputValues);
        //Then convert the data according to the corresponding field type
        for (int i = 0; i < size; i++) {
            Object outputValue = transformValueByType(seaTunnelRowType.getFieldNames()[i], seaTunnelRowType.getFieldType(i), jsonNode.get(i));
            fieldValues[i] = outputValue;
        }
        log.info("Output python script transform:{}", Arrays.toString(fieldValues));
        return fieldValues;
    }

    @Override
    protected Column[] getOutputColumns() {
        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();
        return columns.toArray(new Column[0]);
    }

    /**
     * transform value
     * @param field
     * @param inputDataType
     * @param value
     * @return
     */
    private Object transformValueByType(String field, SeaTunnelDataType<?> inputDataType, Object value) {
        if (value == null) {
            return null;
        }
        switch (inputDataType.getSqlType()) {
            case BOOLEAN:
            case STRING:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return value;
            case BYTES:
                byte[] bytes = (byte[]) value;
                byte[] newBytes = new byte[bytes.length];
                System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                return newBytes;
            case ARRAY:
                ArrayType arrayType = (ArrayType) inputDataType;
                Object[] array = (Object[]) value;
                Object newArray =
                        Array.newInstance(arrayType.getElementType().getTypeClass(), array.length);
                for (int i = 0; i < array.length; i++) {
                    Array.set(newArray, i, transformValueByType(field, arrayType.getElementType(), array[i]));
                }
                return newArray;
            case MAP:
                MapType mapType = (MapType) inputDataType;
                Map map = (Map) value;
                Map<Object, Object> newMap = new HashMap<>();
                for (Object key : map.keySet()) {
                    newMap.put(
                            transformValueByType(field, mapType.getKeyType(), key),
                            transformValueByType(field, mapType.getValueType(), map.get(key)));
                }
                return newMap;
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) inputDataType;
                SeaTunnelRow row = (SeaTunnelRow) value;

                Object[] newFields = new Object[rowType.getTotalFields()];
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    newFields[i] =
                            transformValueByType(
                                    rowType.getFieldName(i),
                                    rowType.getFieldType(i),
                                    row.getField(i));
                }
                SeaTunnelRow newRow = new SeaTunnelRow(newFields);
                newRow.setRowKind(row.getRowKind());
                newRow.setTableId(row.getTableId());
                return newRow;
            case NULL:
                return null;
            default:
                throw CommonError.unsupportedDataType(
                        getPluginName(), inputDataType.getSqlType().toString(), field);
        }
    }


    /**
     * Get fileContent by url
     *
     * @return
     */
    private String getFileContent() {
        String configOption = Options.key(PYTHON_SCRIPT_FILE_ID.key()).stringType()
                .defaultValue(pythonScriptFileId).toString();
        try {
            Properties prop = new Properties();
            InputStream input = PythonTransform.class.getClassLoader().getResourceAsStream("config.properties");
            prop.load(input);

            String url = prop.getProperty(DOWNLOAD_URL);
            if (StringUtils.isEmpty(url)) {
                log.error("[PythonTransform]get download file url is null");
                throw TransformCommonError.initTransformError(PythonTransform.PLUGIN_NAME, configOption);
            }
            String finalUrl = url.replace("${id}", pythonScriptFileId);
            //send http request
            String response = HttpClientUtil.sendGetRequest(finalUrl);
            if (StringUtils.isEmpty(response)) {
                log.error("[PythonTransform]can not find python script content by bp-file-server,url={}", finalUrl);
                throw TransformCommonError.initTransformError(PythonTransform.PLUGIN_NAME, configOption);
            }
            return response;
        } catch (IOException e) {
            log.error("[PythonTransform]PythonTransform init error", e);
            throw TransformCommonError.initTransformError(PythonTransform.PLUGIN_NAME, configOption);
        }
    }

    /**
     * execute python script
     * @param inputValue
     * @return
     */
    public JsonNode exexcutePython(List<Object> inputValue) {
        try {
            //First method:file stream to python process
            ProcessBuilder processBuilder = new ProcessBuilder(PATH_PYTHON_BIN, "-c", pythonScriptContent);
            //Second method:create temp file
            //            this.tempScriptFile = File.createTempFile("process", ".py");
//            try (BufferedWriter bufferedWriter = Files.newBufferedWriter(tempScriptFile.toPath())) {
//                bufferedWriter.write(pythonScriptContent);
//            }
//            ProcessBuilder processBuilder = new ProcessBuilder(PATH_PYTHON_BIN, tempScriptFile.getAbsolutePath());
            pythonProcess = processBuilder.start();
            processBuilder.redirectErrorStream(true);
            writer = new PrintWriter(new OutputStreamWriter(pythonProcess.getOutputStream()), true);
            reader = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));

            //Convert to json array as Python input
            String jsonInput = objectMapper.writeValueAsString(inputValue);
            writer.println(jsonInput);
            writer.close();

            // Read output from Python process
            JsonNode jsonNode = objectMapper.readTree(reader);
            log.info("Python script output (JSON): {}", jsonNode.toString());
            // Wait for Python process to complete
            int exitCode = pythonProcess.waitFor();
            log.info("Python process exited with code={}", exitCode);
            return jsonNode;
        } catch (IOException | InterruptedException e) {
            log.error("[PythonTransform] transform by inputRow error,python_script_file_id={}", pythonScriptFileId, e);
            throw TransformCommonError.executeTransformError(PythonTransform.PLUGIN_NAME, inputValue.toString());
        }
    }
}

