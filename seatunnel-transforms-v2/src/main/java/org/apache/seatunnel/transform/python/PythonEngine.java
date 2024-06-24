package org.apache.seatunnel.transform.python;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.transform.common.HttpClientUtil;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.seatunnel.transform.python.PythonTransform.PYTHON_SCRIPT;

/**
 * @description:
 * @author: wangjing
 * @createDate: 2024/6/20
 */


public class PythonEngine {

    private final String pythonScriptFileId;
    private Process pythonProcess;
    private PrintWriter writer;
    private BufferedReader reader;
    private static final String PATH_PYTHON = "/usr/bin/python3";
    //    private static final String PATH_PYTHON = "C:\\software\\Python3\\python.exe";
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final String TEMP_SCRIPT = "temp_script";
    private final String PYTHON_SUFFIX = ".py";

    private File tempScriptFile;

    public PythonEngine(String pythonScriptFileId) {
        this.pythonScriptFileId = pythonScriptFileId;
    }

    public void init() {
        if (StringUtils.isEmpty(pythonScriptFileId)) {
            throw TransformCommonError.cannotFindInputFieldError(PythonTransform.PLUGIN_NAME, PYTHON_SCRIPT.toString());
        }
        try {
            //get file by pythonScriptFileId
            Properties prop = new Properties();
            InputStream input = PythonEngine.class.getClassLoader().getResourceAsStream("config.properties");
            prop.load(input);

            String url = prop.getProperty("bp-file-server.download.url");
            if (StringUtils.isEmpty(url)) {
                throw TransformCommonError.cannotFINDFileError(PythonTransform.PLUGIN_NAME, pythonScriptFileId);
            }
            String finalUrl = url.replace("${id}", pythonScriptFileId);
            System.out.println("fileUrl:" + finalUrl);
            String response = HttpClientUtil.sendGetRequest(finalUrl);
            if (null == response) {
                throw TransformCommonError.cannotFINDFileError(PythonTransform.PLUGIN_NAME, pythonScriptFileId);
            }
            String seaTunnelHome = Common.getSeaTunnelHome()+File.separator;
            tempScriptFile = File.createTempFile(TEMP_SCRIPT, PYTHON_SUFFIX,new File(seaTunnelHome));
            new BufferedWriter(new FileWriter(tempScriptFile)).write(response);
            // Start Python process
            ProcessBuilder processBuilder = new ProcessBuilder(PATH_PYTHON, seaTunnelHome+TEMP_SCRIPT+PYTHON_SUFFIX);
            System.out.println("python command:"+seaTunnelHome+TEMP_SCRIPT+PYTHON_SUFFIX);
            processBuilder.redirectErrorStream(true); // Merge error stream with standard output
            pythonProcess = processBuilder.start();
            writer = new PrintWriter(new OutputStreamWriter(pythonProcess.getOutputStream()));
            reader = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));

        } catch (IOException e) {
            throw TransformCommonError.startPythonError(PythonTransform.PLUGIN_NAME, pythonScriptFileId);
        }
    }

    public SeaTunnelRow transformByPython(SeaTunnelRow inputRow) {
        List<Object> outputList;
        try {
            // Communicate with Python process
            String jsonInput = objectMapper.writeValueAsString(Arrays.asList(inputRow.getFields()));
            System.out.println("input value" + jsonInput);
            writer.println(jsonInput);
            writer.close();

            // Read output from Python process
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line);
            }
            String jsonOutput = output.toString();
            System.out.println("Python script output (JSON): " + jsonOutput);
            outputList = objectMapper.readValue(jsonOutput, new TypeReference<List<Object>>() {
            });

            // Wait for Python process to complete
            int exitCode = pythonProcess.waitFor();
            System.out.println("Python process exited with code " + exitCode);
        } catch (IOException | InterruptedException e) {
            throw TransformCommonError.transformPythonError(PythonTransform.PLUGIN_NAME, pythonScriptFileId);
        }
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(outputList.toArray(new Object[0]));
        System.out.println("prints seatunnel:" + seaTunnelRow);
        return seaTunnelRow;
    }

    public void close() {
        try {
            tempScriptFile.delete();
            writer.close();
            reader.close();
            pythonProcess.destroy();
        } catch (IOException e) {
            throw new RuntimeException("Error closing Python process", e);
        }
    }


}
