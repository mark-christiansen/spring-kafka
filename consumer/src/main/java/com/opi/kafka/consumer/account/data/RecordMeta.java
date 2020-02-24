package com.opi.kafka.consumer.account.data;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class RecordMeta {
    private byte[] checksum;
    private int version;
}