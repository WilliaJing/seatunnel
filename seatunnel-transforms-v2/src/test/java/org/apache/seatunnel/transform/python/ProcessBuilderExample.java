package org.apache.seatunnel.transform.python;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProcessBuilderExample {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        try {
            Object[] inputData = new Object[4];
            inputData[0] = 1;
            inputData[1] = "233";
            inputData[2] = "2";
            inputData[3] = "234qq.com";


            String prefix = "/Users/wjing/seatunnel/seatunnel-transforms-v2/src/test/java/org/apache/seatunnel/transform/python/";
            // Start Python process
            ProcessBuilder pb = new ProcessBuilder("/usr/local/bin/python3", prefix+"process_data.py");
            pb.redirectErrorStream(true); // Merge error stream with standard output
            Process process = pb.start();

            StringBuilder outputBuilder = new StringBuilder();
            // Communicate with Python process
            try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(process.getOutputStream()));
                 BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {

                // Send input data to Python process line by line
                for (Object line : inputData) {
                    System.out.println("received line"+line.toString());
                    writer.println(line);
                }
                writer.close(); // Close writer to indicate end of input

                // Read output from Python process
                String line;
                while ((line = reader.readLine()) != null) {
                    outputBuilder.append(line);
                }

            }
            // Wait for Python process to complete
            int exitCode = process.waitFor();
            System.out.println("Python process exited with code " + exitCode);
            // 输出脚本的标准输出
            String jsonOutput = outputBuilder.toString();
            System.out.println("Python script output (JSON): " + jsonOutput);
            List<Object> outputList = objectMapper.readValue(jsonOutput, new TypeReference<List<Object>>(){});
            for (Object obj : outputList) {
                System.out.println(obj.getClass());
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}