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

    private final String pythonScriptFileId;
    private Process pythonProcess;
    private PrintWriter writer;
    private BufferedReader reader;
    private static final String PATH_PYTHON = "/usr/bin/python3";
    //    private static final String PATH_PYTHON = "C:\\software\\Python3\\python.exe";
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PythonEngine(String pythonScriptFileId) {
        this.pythonScriptFileId = pythonScriptFileId;
    }

    public void init() {
        System.out.println("init pythonTransform.......");
        if (StringUtils.isEmpty(pythonScriptFileId)) {
            log.error("[PythonTransform]python_script_file_id is must be not null");
            throw TransformCommonError.initTransformError(PythonTransform.PLUGIN_NAME, null);
        }
        String configOption = Options.key(PYTHON_SCRIPT_FILE_ID.key()).stringType()
                .defaultValue(pythonScriptFileId).toString();
        try {
            // Start Python process
            ProcessBuilder processBuilder = new ProcessBuilder(PATH_PYTHON, "-c", "import sys; exec(sys.stdin.read())");
            processBuilder.redirectErrorStream(true);
            pythonProcess = processBuilder.start();

            writer = new PrintWriter(new OutputStreamWriter(pythonProcess.getOutputStream()), true);
            reader = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));

            //get fileStream by pythonScriptFileId
            Properties prop = new Properties();
            InputStream input = PythonTransform.class.getClassLoader().getResourceAsStream("config.properties");
            prop.load(input);

            String url = prop.getProperty("bp-file-server.download.url");
            if (StringUtils.isEmpty(url)) {
                log.error("[PythonTransform]get download file url is null");
                throw TransformCommonError.initTransformError(PythonTransform.PLUGIN_NAME, configOption);
            }
            String finalUrl = url.replace("${id}", pythonScriptFileId);
            log.info("call url={}", finalUrl);
            String response = HttpClientUtil.sendGetRequest(finalUrl);
            if (StringUtils.isEmpty(response)) {
                log.error("[PythonTransform]can not find python script content by bp-file-server,url={}", finalUrl);
                throw TransformCommonError.initTransformError(PythonTransform.PLUGIN_NAME, configOption);
            }
            log.info("GetPythonScriptFileContent={}", response);

            // 把获取的脚本内容写入子进程
            writer.write(response);
            if (writer == null) {
                log.info("write is nulllll,{}", response);
            }
        } catch (IOException e) {
            log.error("[PythonTransform]PythonTransform init error", e);
            throw TransformCommonError.initTransformError(PythonTransform.PLUGIN_NAME, configOption);
        }
    }

    public SeaTunnelRow transformByPython(SeaTunnelRow inputRow) {
        System.out.println("pythonTransform.......");
        List<Object> outputList;
        try {
            // Communicate with Python process
            String jsonInput = objectMapper.writeValueAsString(Arrays.asList(inputRow.getFields()));
            log.info("input value={}", jsonInput);
            writer.println(jsonInput);
            writer.close();

            // Read output from Python process
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                log.info("output line={}", line);
                output.append(line);
            }
            String jsonOutput = output.toString();
            outputList = objectMapper.readValue(jsonOutput, new TypeReference<List<Object>>() {
            });

            // Wait for Python process to complete
            int exitCode = pythonProcess.waitFor();
            log.info("Python process exited with code={}", exitCode);
        } catch (IOException | InterruptedException e) {
            log.error("[PythonTransform] transform by inputRow error", e);
            throw TransformCommonError.executeTransformError(PythonTransform.PLUGIN_NAME, inputRow.toString());
        }
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(outputList.toArray(new Object[0]));
        System.out.println("prints seatunnel:" + seaTunnelRow);
        return seaTunnelRow;
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
    }

}
