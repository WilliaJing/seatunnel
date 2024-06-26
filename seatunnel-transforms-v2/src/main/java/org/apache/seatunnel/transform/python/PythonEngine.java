package org.apache.seatunnel.transform.python;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.transform.common.HttpClientUtil;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.seatunnel.transform.python.PythonTransform.PYTHON_SCRIPT_FILE_ID;

/**
 * @description:
 * @author: wangjing
 * @createDate: 2024/6/20
 */
@Slf4j
public class PythonEngine {

    private String pythonScriptFileId;
    private Process pythonProcess;
    private PrintWriter writer;
    private BufferedReader reader;
    private static final String PATH_PYTHON_BIN = "/usr/bin/python3";
    //    private static final String PATH_PYTHON_BIN = "C:\\software\\Python3\\python.exe";
    private final ObjectMapper objectMapper = new ObjectMapper();

    private String pythonScriptContent;

    private File tempScriptFile;

    private String configOption;

    private static final String DOWNLOAD_URL = "bp-file-server.download.url";


    public PythonEngine() {
    }

    public void init(String pythonScriptFileId) {
        if (StringUtils.isEmpty(pythonScriptFileId)) {
            log.error("[PythonTransform]python_script_file_id is must be not null");
            throw TransformCommonError.initTransformError(PythonTransform.PLUGIN_NAME, null);
        }
        this.pythonScriptFileId = pythonScriptFileId;
        this.configOption = Options.key(PYTHON_SCRIPT_FILE_ID.key()).stringType()
                .defaultValue(pythonScriptFileId).toString();
        // get python script file
        this.pythonScriptContent = getFileContent();
        log.info("PythonScriptFileContent={}", pythonScriptContent);
    }

    public SeaTunnelRow transformByPython(SeaTunnelRow inputRow) {
        List<Object> outputList;
        try {
            //one way:file stream to python process
            ProcessBuilder processBuilder = new ProcessBuilder(PATH_PYTHON_BIN, "-c", pythonScriptContent);
            //two way:create temp file
//            this.tempScriptFile = File.createTempFile("process", ".py");
//            try (BufferedWriter bufferedWriter = Files.newBufferedWriter(tempScriptFile.toPath())) {
//                bufferedWriter.write(pythonScriptContent);
//            }
//            ProcessBuilder processBuilder = new ProcessBuilder(PATH_PYTHON_BIN, tempScriptFile.getAbsolutePath());

            pythonProcess = processBuilder.start();
            processBuilder.redirectErrorStream(true);
            writer = new PrintWriter(new OutputStreamWriter(pythonProcess.getOutputStream()), true);
            reader = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));


            String jsonInput = objectMapper.writeValueAsString(Arrays.asList(inputRow.getFields()));
            writer.println(jsonInput);
            writer.close();

            // Read output from Python process
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line);
            }
            String jsonOutput = output.toString();
            log.info("Python script output (JSON): {}", jsonOutput);
            outputList = objectMapper.readValue(jsonOutput, new TypeReference<List<Object>>() {
            });

            // Wait for Python process to complete
            int exitCode = pythonProcess.waitFor();
            log.info("Python process exited with code={}", exitCode);
        } catch (IOException | InterruptedException e) {
            log.error("[PythonTransform] transform by inputRow error,python_script_file_id={}", pythonScriptFileId, e);
            throw TransformCommonError.executeTransformError(PythonTransform.PLUGIN_NAME, inputRow.toString());
        }
        return new SeaTunnelRow(outputList.toArray(new Object[0]));
    }

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
        tempScriptFile.deleteOnExit();
    }

    /**
     * Get fileContent by url
     *
     * @return
     */
    private String getFileContent() {
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

}
