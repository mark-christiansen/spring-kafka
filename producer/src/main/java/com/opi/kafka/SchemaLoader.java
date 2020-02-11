package com.opi.kafka;

import org.apache.avro.Schema;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchemaLoader {
    public Schema getSchema(String schemaFilepath) throws URISyntaxException, IOException {
        Path path = Paths.get(getClass().getClassLoader().getResource(schemaFilepath).toURI());
        Stream<String> lines = Files.lines(path).map(str -> str.trim());
        String data = lines.collect(Collectors.joining(""));
        return new Schema.Parser().parse(data);
    }
}