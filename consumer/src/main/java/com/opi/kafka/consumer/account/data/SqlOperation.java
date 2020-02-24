package com.opi.kafka.consumer.account.data;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter @Setter
public class SqlOperation {
    private final String table;
    private final String findSql;
    private final String modifySql;
    private final StatementSetter setter;
    private final boolean insert;
    private final ChecksumWriter checksumWriter;
}
