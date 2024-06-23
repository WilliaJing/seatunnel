package org.apache.seatunnel.transform.python;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.SeaTunnelRowContainerGenerator;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @description:
 * @author: wangjing
 * @createDate: 2024/6/20
 */


public class PythonEngine {

    private final String scriptPath;
    private Process pythonProcess;
    private PrintWriter writer;
    private BufferedReader reader;
//    private static final String PATH_PYTHON = "/usr/local/bin/python3";
    private static final String PATH_PYTHON = "/usr/bin/python3";
//    private static final String PATH_PYTHON = "C:\\software\\Python3\\python.exe";
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PythonEngine(String scriptPath) {
        this.scriptPath = scriptPath;
    }

    public void init() {
        try {
            // Start Python process
            ProcessBuilder processBuilder = new ProcessBuilder(PATH_PYTHON, scriptPath);
            processBuilder.redirectErrorStream(true); // Merge error stream with standard output
            pythonProcess = processBuilder.start();
            writer = new PrintWriter(new OutputStreamWriter(pythonProcess.getOutputStream()));
            reader = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));

        } catch (IOException e) {
            throw new RuntimeException("Failed to start Python process", e);
        }
    }

    public SeaTunnelRow transformByPython(SeaTunnelRow inputRow) {
        List<Object> outputList;
        try {
            // Communicate with Python process
            String jsonInput = objectMapper.writeValueAsString(Arrays.asList(inputRow.getFields()));
            System.out.println("input value"+jsonInput);
            writer.println(jsonInput);
            writer.close();

            // Read output from Python process
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("Read line: " + line);
                output.append(line);
            }
            String jsonOutput = output.toString();
            System.out.println("Python script output (JSON): " + jsonOutput);
            outputList = objectMapper.readValue(jsonOutput, new TypeReference<List<Object>>() {
            });

            // Wait for Python process to complete
            int exitCode = pythonProcess.waitFor();
            System.out.println("Python process exited with code " + exitCode);
            // Wait for Python process to complete
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error transformByPython", e);
        }
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(outputList.toArray(new Object[0]));
        System.out.println("prints seatunnel:"+ seaTunnelRow);
        return seaTunnelRow;
    }

    public void close() {
        try {
            writer.close();
            reader.close();
            pythonProcess.destroy();
        } catch (IOException e) {
            throw new RuntimeException("Error closing Python process", e);
        }
    }


}
