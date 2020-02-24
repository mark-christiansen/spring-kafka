package com.opi.kafka.consumer.account.data;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;

@AllArgsConstructor
@Slf4j
public class UpsertOperation implements Runnable {

    private final List<GenericRecord> records;
    private final Connection conn;
    private final SqlOperation insertOp;
    private final SqlOperation updateOp;
    private final CountDownLatch latch;
    private final ExecutorService executorService;
    private final UUID breadCrumbId;

    @SneakyThrows
    @Override
    public void run() {

        // remove earlier records in the list with duplicate ids because each record has current state (keeps the
        // latest in the list)
        Set<Long> ids = new HashSet<>();
        for (int i = records.size() - 1; i >= 0; i--) {
            GenericRecord record = records.get(i);
            if (ids.contains(record.get("id"))) {
                records.remove(i);
            } else {
                ids.add((Long) record.get("id"));
            }
        }

        if (records.isEmpty()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        Map<Long, RecordMeta> recordMeta = getRecordMeta(conn, records, insertOp.getFindSql());
        //log.info("{}  -> get record meta for \"{}\" took {} ms", breadCrumbId, insertOp.getTable(), System.currentTimeMillis() - startTime);

        // Assume that there is no relationship between inserts/updates so they can be executed in parallel.
        // In this case, the table is not generating primary keys, they are being passed in the incoming record so that
        // doesn't prevent parallelization of these SQL executions.
        CountDownLatch upsertLatch = new CountDownLatch(2);
        ModifySqlOperation insert = new ModifySqlOperation(recordMeta, records, conn, insertOp, upsertLatch, breadCrumbId);
        ModifySqlOperation update = new ModifySqlOperation(recordMeta, records, conn, updateOp, upsertLatch, breadCrumbId);

        startTime = System.currentTimeMillis();
        executorService.execute(insert);
        executorService.execute(update);
        upsertLatch.await(20, TimeUnit.SECONDS);
        //log.info("{}  -> insert/update for \"{}\" took {} ms", breadCrumbId, insertOp.getTable(), System.currentTimeMillis() - startTime);

        if (latch != null ) {
            latch.countDown();
        }
    }

    private Map<Long, RecordMeta> getRecordMeta(Connection conn, List<GenericRecord> records, String findSql) throws SQLException {

        Map<Long, RecordMeta> existingRecords = new HashMap<>();
        try (Statement findStmt = conn.createStatement()) {

            String idString = records.stream().map(r -> ((Long) r.get("id")).toString()).collect(Collectors.joining(","));
            ResultSet rs = findStmt.executeQuery(format(findSql, idString));
            while (rs.next()) {
                existingRecords.put(rs.getLong("id"), new RecordMeta(rs.getBytes("checksum"), rs.getInt("version")));
            }
            rs.close();
        }
        return existingRecords;
    }
}