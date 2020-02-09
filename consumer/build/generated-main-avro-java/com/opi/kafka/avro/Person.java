/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.opi.kafka.avro;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

/** A person */
@org.apache.avro.specific.AvroGenerated
public class Person extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 6283541477898904402L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Person\",\"namespace\":\"com.opi.kafka.avro\",\"doc\":\"A person\",\"fields\":[{\"name\":\"name\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"Person full name\"},{\"name\":\"address\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"Person home address\",\"default\":null},{\"name\":\"age\",\"type\":[\"null\",\"int\"],\"doc\":\"Person age is years\",\"default\":null}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<Person> ENCODER =
      new BinaryMessageEncoder<Person>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<Person> DECODER =
      new BinaryMessageDecoder<Person>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<Person> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<Person> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<Person> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<Person>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this Person to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a Person from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a Person instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static Person fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  /** Person full name */
  @Deprecated public java.lang.String name;
  /** Person home address */
  @Deprecated public java.lang.String address;
  /** Person age is years */
  @Deprecated public java.lang.Integer age;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Person() {}

  /**
   * All-args constructor.
   * @param name Person full name
   * @param address Person home address
   * @param age Person age is years
   */
  public Person(java.lang.String name, java.lang.String address, java.lang.Integer age) {
    this.name = name;
    this.address = address;
    this.age = age;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return name;
    case 1: return address;
    case 2: return age;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: name = (java.lang.String)value$; break;
    case 1: address = (java.lang.String)value$; break;
    case 2: age = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'name' field.
   * @return Person full name
   */
  public java.lang.String getName() {
    return name;
  }


  /**
   * Sets the value of the 'name' field.
   * Person full name
   * @param value the value to set.
   */
  public void setName(java.lang.String value) {
    this.name = value;
  }

  /**
   * Gets the value of the 'address' field.
   * @return Person home address
   */
  public java.lang.String getAddress() {
    return address;
  }


  /**
   * Sets the value of the 'address' field.
   * Person home address
   * @param value the value to set.
   */
  public void setAddress(java.lang.String value) {
    this.address = value;
  }

  /**
   * Gets the value of the 'age' field.
   * @return Person age is years
   */
  public java.lang.Integer getAge() {
    return age;
  }


  /**
   * Sets the value of the 'age' field.
   * Person age is years
   * @param value the value to set.
   */
  public void setAge(java.lang.Integer value) {
    this.age = value;
  }

  /**
   * Creates a new Person RecordBuilder.
   * @return A new Person RecordBuilder
   */
  public static com.opi.kafka.avro.Person.Builder newBuilder() {
    return new com.opi.kafka.avro.Person.Builder();
  }

  /**
   * Creates a new Person RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Person RecordBuilder
   */
  public static com.opi.kafka.avro.Person.Builder newBuilder(com.opi.kafka.avro.Person.Builder other) {
    if (other == null) {
      return new com.opi.kafka.avro.Person.Builder();
    } else {
      return new com.opi.kafka.avro.Person.Builder(other);
    }
  }

  /**
   * Creates a new Person RecordBuilder by copying an existing Person instance.
   * @param other The existing instance to copy.
   * @return A new Person RecordBuilder
   */
  public static com.opi.kafka.avro.Person.Builder newBuilder(com.opi.kafka.avro.Person other) {
    if (other == null) {
      return new com.opi.kafka.avro.Person.Builder();
    } else {
      return new com.opi.kafka.avro.Person.Builder(other);
    }
  }

  /**
   * RecordBuilder for Person instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Person>
    implements org.apache.avro.data.RecordBuilder<Person> {

    /** Person full name */
    private java.lang.String name;
    /** Person home address */
    private java.lang.String address;
    /** Person age is years */
    private java.lang.Integer age;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.opi.kafka.avro.Person.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.address)) {
        this.address = data().deepCopy(fields()[1].schema(), other.address);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.age)) {
        this.age = data().deepCopy(fields()[2].schema(), other.age);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    /**
     * Creates a Builder by copying an existing Person instance
     * @param other The existing instance to copy.
     */
    private Builder(com.opi.kafka.avro.Person other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.address)) {
        this.address = data().deepCopy(fields()[1].schema(), other.address);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.age)) {
        this.age = data().deepCopy(fields()[2].schema(), other.age);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'name' field.
      * Person full name
      * @return The value.
      */
    public java.lang.String getName() {
      return name;
    }


    /**
      * Sets the value of the 'name' field.
      * Person full name
      * @param value The value of 'name'.
      * @return This builder.
      */
    public com.opi.kafka.avro.Person.Builder setName(java.lang.String value) {
      validate(fields()[0], value);
      this.name = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'name' field has been set.
      * Person full name
      * @return True if the 'name' field has been set, false otherwise.
      */
    public boolean hasName() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'name' field.
      * Person full name
      * @return This builder.
      */
    public com.opi.kafka.avro.Person.Builder clearName() {
      name = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'address' field.
      * Person home address
      * @return The value.
      */
    public java.lang.String getAddress() {
      return address;
    }


    /**
      * Sets the value of the 'address' field.
      * Person home address
      * @param value The value of 'address'.
      * @return This builder.
      */
    public com.opi.kafka.avro.Person.Builder setAddress(java.lang.String value) {
      validate(fields()[1], value);
      this.address = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'address' field has been set.
      * Person home address
      * @return True if the 'address' field has been set, false otherwise.
      */
    public boolean hasAddress() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'address' field.
      * Person home address
      * @return This builder.
      */
    public com.opi.kafka.avro.Person.Builder clearAddress() {
      address = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'age' field.
      * Person age is years
      * @return The value.
      */
    public java.lang.Integer getAge() {
      return age;
    }


    /**
      * Sets the value of the 'age' field.
      * Person age is years
      * @param value The value of 'age'.
      * @return This builder.
      */
    public com.opi.kafka.avro.Person.Builder setAge(java.lang.Integer value) {
      validate(fields()[2], value);
      this.age = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'age' field has been set.
      * Person age is years
      * @return True if the 'age' field has been set, false otherwise.
      */
    public boolean hasAge() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'age' field.
      * Person age is years
      * @return This builder.
      */
    public com.opi.kafka.avro.Person.Builder clearAge() {
      age = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Person build() {
      try {
        Person record = new Person();
        record.name = fieldSetFlags()[0] ? this.name : (java.lang.String) defaultValue(fields()[0]);
        record.address = fieldSetFlags()[1] ? this.address : (java.lang.String) defaultValue(fields()[1]);
        record.age = fieldSetFlags()[2] ? this.age : (java.lang.Integer) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<Person>
    WRITER$ = (org.apache.avro.io.DatumWriter<Person>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<Person>
    READER$ = (org.apache.avro.io.DatumReader<Person>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.name);

    if (this.address == null) {
      out.writeIndex(0);
      out.writeNull();
    } else {
      out.writeIndex(1);
      out.writeString(this.address);
    }

    if (this.age == null) {
      out.writeIndex(0);
      out.writeNull();
    } else {
      out.writeIndex(1);
      out.writeInt(this.age);
    }

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.name = in.readString();

      if (in.readIndex() != 1) {
        in.readNull();
        this.address = null;
      } else {
        this.address = in.readString();
      }

      if (in.readIndex() != 1) {
        in.readNull();
        this.age = null;
      } else {
        this.age = in.readInt();
      }

    } else {
      for (int i = 0; i < 3; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.name = in.readString();
          break;

        case 1:
          if (in.readIndex() != 1) {
            in.readNull();
            this.address = null;
          } else {
            this.address = in.readString();
          }
          break;

        case 2:
          if (in.readIndex() != 1) {
            in.readNull();
            this.age = null;
          } else {
            this.age = in.readInt();
          }
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










