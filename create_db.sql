CREATE TABLE account (
    id BIGINT NOT NULL,
    first_name VARCHAR(100) NULL,
    middle_name VARCHAR(100) NULL,
    last_name VARCHAR(100) NULL,
    birth_date DATE NULL,
    salary DOUBLE NULL,
    checksum BINARY(16) NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE address (
    id BIGINT NOT NULL,
    person_id BIGINT NOT NULL,
    line_1 VARCHAR(300) NOT NULL,
    line_2 VARCHAR(300) NULL,
    line_3 VARCHAR(300) NULL,
    city VARCHAR(150) NOT NULL,
    state VARCHAR(100) NOT NULL,
    country VARCHAR(150) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    checksum BINARY(16) NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT FOREIGN KEY (person_id) REFERENCES account (id)
);

CREATE TABLE phone (
    id BIGINT NOT NULL,
    person_id BIGINT NOT NULL,
    type VARCHAR(100) NOT NULL,
    area_code INT NOT NULL,
    number INT NOT NULL,
    extension INT NULL,
    checksum BINARY(16) NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT FOREIGN KEY (person_id) REFERENCES account (id)
);