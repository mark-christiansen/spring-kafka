package com.opi.kafka.consumer.account.data;

import com.opi.kafka.consumer.account.GenericRecordStatement;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Slf4j
@AllArgsConstructor
public class ModifySqlOperation implements Runnable {

    private final Map<Long, RecordMeta> recordMeta;
    private final List<GenericRecord> records;
    private final Connection conn;
    private final SqlOperation op;
    private final CountDownLatch latch;
    private final UUID breadCrumbId;

    @SneakyThrows
    @Override
    public void run() {

        StatementCache cache = StatementCache.getInstance();

        List<GenericRecord> recsToModify = records.stream()
                .filter(a -> op.isInsert() != recordMeta.containsKey(a.get("id")))
                .collect(Collectors.toList());
        ChecksumWriter checksumWriter = op.getChecksumWriter();

        GenericRecordStatement stmt = cache.getStatement(conn, op.getModifySql());
        for (GenericRecord r : recsToModify) {
            try {

                int version = op.isInsert() ? 1 : recordMeta.get(r.get("id")).getVersion() + 1;
                byte[] checksum = checksumWriter.checksum(r);

                if (op.isInsert() || !recordMeta.get(r.get("id")).getChecksum().equals(checksum)) {
                    stmt.clearParameters();
                    op.getSetter().set(stmt, r, checksum, version);
                    stmt.addBatch();
                }

            } catch (Exception e) {
                log.error(format("Error executing \"%s\" batch", op.getTable()), e);
                return;
            }
        }

        long startTime = System.currentTimeMillis();
        stmt.executeBatch();
        //log.info("{}    -> execute pstmt for \"{}\" took {} ms", breadCrumbId, op.getTable(), System.currentTimeMillis() - startTime);

        latch.countDown();
    }
}