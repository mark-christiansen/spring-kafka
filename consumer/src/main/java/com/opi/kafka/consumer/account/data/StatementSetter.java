package com.opi.kafka.consumer.account.data;

import com.opi.kafka.consumer.account.GenericRecordStatement;
import org.apache.avro.generic.GenericRecord;

import java.sql.SQLException;

public interface StatementSetter {
    void set(GenericRecordStatement stmt, GenericRecord record, byte[] checksum, int version) throws SQLException;
}