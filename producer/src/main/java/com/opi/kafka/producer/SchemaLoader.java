package com.opi.kafka.producer;

import org.apache.avro.Schema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class SchemaLoader {
    public Schema getSchema(String schemaFilepath) throws IOException {

        StringBuilder sb = new StringBuilder();
        try(InputStream in = getClass().getClassLoader().getResourceAsStream(schemaFilepath);
            BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

            String line = null;
            while((line = br.readLine()) != null) {
                sb.append(line.trim());
            }
        }
        return new Schema.Parser().parse(sb.toString());
    }
}