package com.opi.kafka.consumer.account.data;

import com.twmacinta.util.MD5;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ChecksumWriter {

    private final DatumWriter<GenericRecord> writer;

    public ChecksumWriter(Schema schema) {
        this.writer = new GenericDatumWriter<>(schema);
    }

    public byte[] checksum(GenericRecord record) throws IOException {
        MD5 md = new MD5();
        md.Update(bytes(record));
        return md.Final();
    }

    public byte[] bytes(GenericRecord record) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        this.writer.write(record, encoder);
        encoder.flush();
        return baos.toByteArray();
    }
}