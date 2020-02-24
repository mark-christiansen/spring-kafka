package com.opi.kafka.consumer.account;

import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class GenericRecordStatement implements Closeable {

    private final PreparedStatement stmt;

    public GenericRecordStatement(PreparedStatement stmt) {
        this.stmt = stmt;
    }

    public void setString(int i, Utf8 value) throws SQLException {
        if (value != null) {
            stmt.setString(i, value.toString());
        } else {
            stmt.setNull(i, Types.VARCHAR);
        }
    }

    public void setString(int i, GenericData.EnumSymbol value) throws SQLException {
        if (value != null) {
            stmt.setString(i, value.toString());
        } else {
            stmt.setNull(i, Types.VARCHAR);
        }
    }

    public void setInt(int i, Integer value) throws SQLException {
        if (value != null) {
            stmt.setInt(i, value);
        } else {
            stmt.setNull(i, Types.INTEGER);
        }
    }

    public void setDouble(int i, Double value) throws SQLException {
        if (value != null) {
            stmt.setDouble(i, value);
        } else {
            stmt.setNull(i, Types.DOUBLE);
        }
    }

    public void setDate(int i, Integer value) throws SQLException {
        if (value != null) {
            // Avro int dates are seconds since epoch so convert to milliseconds for Java dates
            stmt.setDate(i, new Date(value * 1000));
        } else {
            stmt.setNull(i, Types.DATE);
        }
    }

    public void setLong(int i, Long value) throws SQLException {
        if (value != null) {
            stmt.setLong(i, value);
        } else {
            stmt.setNull(i, Types.BIGINT);
        }
    }

    public void setBytes(int i, byte[] value) throws SQLException {
        if (value != null) {
            stmt.setBytes(i, value);
        } else {
            stmt.setNull(i, Types.BINARY);
        }
    }

    public void clearParameters() throws SQLException {
        this.stmt.clearParameters();
    }

    public void addBatch() throws SQLException {
        this.stmt.addBatch();
    }

    public int[] executeBatch() throws SQLException {
        return this.stmt.executeBatch();
    }

    @Override
    public void close() throws IOException {
        try {
            stmt.close();
        } catch (SQLException e) {
            throw new IOException("Exception closing prepared statement", e);
        }
    }
}
