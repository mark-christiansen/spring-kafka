package com.opi.kafka.consumer.account;

import com.opi.kafka.consumer.account.data.ChecksumWriter;
import com.opi.kafka.consumer.account.data.SqlOperation;
import com.opi.kafka.consumer.account.data.UpsertOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class AccountService {

    private static final String FIND_ACCOUNT_IDS = "select id, checksum, version from account where id in (%s)";
    private static final String INSERT_ACCOUNT = "insert into account (id, first_name, middle_name, last_name, birth_date, salary, checksum, version) values (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_ACCOUNT = "update account set first_name=?, middle_name=?, last_name=?, birth_date=?, salary=?, checksum=?, version=? where id=?";

    private static final String FIND_ADDRESS_IDS = "select id, checksum, version from address where id in (%s)";
    private static final String INSERT_ADDRESS = "insert into address (id, person_id, line_1, line_2, line_3, city, state, country, postal_code, checksum, version) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_ADDRESS = "update address set person_id=?, line_1=?, line_2=?, line_3=?, city=?, state=?, country=?, postal_code=?, checksum=?, version=? where id=?";

    private static final String FIND_PHONE_IDS = "select id, checksum, version from phone where id in (%s)";
    private static final String INSERT_PHONE = "insert into phone (id, person_id, type, area_code, number, extension, checksum, version) values (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_PHONE = "update phone set person_id=?, type=?, area_code=?, number=?, extension=?, checksum=?, version=? where id=?";

    private final DataSource datasource;
    private final Schema accountSchema;
    private final Schema addressSchema;
    private final Schema phoneSchema;
    private final ExecutorService executorService = Executors.newFixedThreadPool(50);

    public AccountService(DataSource datasource,
                          Schema accountSchema,
                          Schema addressSchema,
                          Schema phoneSchema) {
        this.datasource = datasource;
        this.accountSchema = accountSchema;
        this.addressSchema = addressSchema;
        this.phoneSchema = phoneSchema;
    }

    public void append(List<GenericRecord> accounts, UUID breadCrumbId) {

        if (accounts.isEmpty()) {
            return;
        }

        try (Connection conn = datasource.getConnection()) {

            // run account upserts first and wait because addresses and phones have foreign keys to account
            SqlOperation accountInsert = new SqlOperation("account", FIND_ACCOUNT_IDS, INSERT_ACCOUNT,
                    (stmt, record, checksum, version) -> {
                        stmt.setLong(1, (Long) record.get("id"));
                        stmt.setString(2, (Utf8) record.get("firstName"));
                        stmt.setString(3, (Utf8) record.get("middleName"));
                        stmt.setString(4, (Utf8) record.get("lastName"));
                        stmt.setDate(5, (Integer) record.get("birthDate"));
                        stmt.setDouble(6, (Double) record.get("salary"));
                        stmt.setBytes(7, checksum);
                        stmt.setInt(8, version);
                    }, true, new ChecksumWriter(accountSchema));
            SqlOperation accountUpdate = new SqlOperation("account", FIND_ACCOUNT_IDS, UPDATE_ACCOUNT,
                    (stmt, record, checksum, version) -> {
                        stmt.setString(1, (Utf8) record.get("firstName"));
                        stmt.setString(2, (Utf8) record.get("middleName"));
                        stmt.setString(3, (Utf8) record.get("lastName"));
                        stmt.setDate(4, (Integer) record.get("birthDate"));
                        stmt.setDouble(5, (Double) record.get("salary"));
                        stmt.setBytes(6, checksum);
                        stmt.setInt(7, version);
                        stmt.setLong(8, (Long) record.get("id"));
                    }, false, new ChecksumWriter(accountSchema));
            UpsertOperation account = new UpsertOperation(accounts, conn, accountInsert, accountUpdate, null, executorService, breadCrumbId);
            long startTime = System.currentTimeMillis();
            account.run();
            log.info("{}-> account took {} ms", breadCrumbId, System.currentTimeMillis() - startTime);

            CountDownLatch latch = new CountDownLatch(2);

            // create upsert for address
            List<GenericRecord> addresses = accounts.stream()
                    .filter(a -> a.get("addresses") != null)
                    .map(a -> (List<GenericRecord>) a.get("addresses"))
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
            SqlOperation addressInsert = new SqlOperation("address", FIND_ADDRESS_IDS, INSERT_ADDRESS,
                    (stmt, record, checksum, version) -> {
                        stmt.setLong(1, (Long) record.get("id"));
                        stmt.setLong(2, (Long) record.get("personId"));
                        stmt.setString(3, (Utf8) record.get("line1"));
                        stmt.setString(4, (Utf8) record.get("line2"));
                        stmt.setString(5, (Utf8) record.get("line3"));
                        stmt.setString(6, (Utf8) record.get("city"));
                        stmt.setString(7, (Utf8) record.get("state"));
                        stmt.setString(8, (Utf8) record.get("country"));
                        stmt.setString(9, (Utf8) record.get("postalCode"));
                        stmt.setBytes(10, checksum);
                        stmt.setInt(11, version);
                    }, true, new ChecksumWriter(addressSchema));
            SqlOperation addressUpdate = new SqlOperation("address", FIND_ADDRESS_IDS, UPDATE_ADDRESS,
                    (stmt, record, checksum, version) -> {
                        stmt.setLong(1, (Long) record.get("personId"));
                        stmt.setString(2, (Utf8) record.get("line1"));
                        stmt.setString(3, (Utf8) record.get("line2"));
                        stmt.setString(4, (Utf8) record.get("line3"));
                        stmt.setString(5, (Utf8) record.get("city"));
                        stmt.setString(6, (Utf8) record.get("state"));
                        stmt.setString(7, (Utf8) record.get("country"));
                        stmt.setString(8, (Utf8) record.get("postalCode"));
                        stmt.setBytes(9, checksum);
                        stmt.setInt(10, version);
                        stmt.setLong(11, (Long) record.get("id"));
                    }, false, new ChecksumWriter(addressSchema));
            UpsertOperation address = new UpsertOperation(addresses, conn, addressInsert, addressUpdate, latch, executorService, breadCrumbId);

            // create upsert for phone
            List<GenericRecord> phones = accounts.stream()
                    .filter(a -> a.get("phones") != null)
                    .map(a -> (List<GenericRecord>) a.get("phones"))
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
            SqlOperation phoneInsert = new SqlOperation("phone", FIND_PHONE_IDS, INSERT_PHONE,
                    (stmt, record, checksum, version) -> {
                        stmt.setLong(1, (Long) record.get("id"));
                        stmt.setLong(2, (Long) record.get("personId"));
                        stmt.setString(3, (GenericData.EnumSymbol) record.get("type"));
                        stmt.setInt(4, (Integer) record.get("areaCode"));
                        stmt.setInt(5, (Integer) record.get("number"));
                        stmt.setInt(6, (Integer) record.get("extension"));
                        stmt.setBytes(7, checksum);
                        stmt.setInt(8, version);
                    }, true, new ChecksumWriter(phoneSchema));
            SqlOperation phoneUpdate = new SqlOperation("phone", FIND_PHONE_IDS, UPDATE_PHONE,
                    (stmt, record, checksum, version) -> {
                        stmt.setLong(1, (Long) record.get("personId"));
                        stmt.setString(2, (GenericData.EnumSymbol) record.get("type"));
                        stmt.setInt(3, (Integer) record.get("areaCode"));
                        stmt.setInt(4, (Integer) record.get("number"));
                        stmt.setInt(5, (Integer) record.get("extension"));
                        stmt.setBytes(6, checksum);
                        stmt.setInt(7, version);
                        stmt.setLong(8, (Long) record.get("id"));
                    }, false, new ChecksumWriter(phoneSchema));
            UpsertOperation phone = new UpsertOperation(phones, conn, phoneInsert, phoneUpdate, latch, executorService, breadCrumbId);

            // execute address and phone upserts in parallel
            startTime = System.currentTimeMillis();
            executorService.execute(address);
            executorService.execute(phone);
            latch.await(20, TimeUnit.SECONDS);
            log.info("{}-> address/phone took {} ms", breadCrumbId, System.currentTimeMillis() - startTime);

        } catch (SQLException | InterruptedException e) {
            log.error("Error upserting batch into database", e);
        }
    }
}