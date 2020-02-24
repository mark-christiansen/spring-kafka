package com.opi.kafka.consumer.account.data;

import com.opi.kafka.consumer.account.GenericRecordStatement;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StatementCache {

    private static StatementCache instance;

    private final Map<Integer, Map<String, GenericRecordStatement>> cache = Collections.synchronizedMap(new HashMap<>());

    public static StatementCache getInstance() {
        if (instance == null) {
            instance = new StatementCache();
        }
        return instance;
    }

    private StatementCache() {
    }

    public GenericRecordStatement getStatement(Connection conn, String sql) throws SQLException {

        Map<String, GenericRecordStatement> stmts = cache.computeIfAbsent(conn.hashCode(), k -> new HashMap<>());
        GenericRecordStatement stmt = stmts.get(sql);
        if (stmt == null) {
            stmt = createStatement(conn, sql);
            stmts.put(sql, stmt);
        }
        return stmt;
    }

    private GenericRecordStatement createStatement(Connection conn, String sql) throws SQLException {
        return new GenericRecordStatement(conn.prepareStatement(sql));
    }
}