/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.examples.jabeja;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Implements helper functions to read and write collections
 */
public abstract class BaseWritable implements Writable {
  /**
   * A default value reader for integers
   */
  protected static final ValueReader<Integer> INTEGER_VALUE_READER =
    new ValueReader<Integer>() {
      @Override
      public Integer readValue(DataInput input) throws IOException {
        return input.readInt();
      }
    };

  /**
   * A default value reader for longs
   */
  protected static final ValueReader<Long> LONG_VALUE_READER =
    new ValueReader<Long>() {
      @Override
      public Long readValue(DataInput input) throws IOException {
        return input.readLong();
      }
    };

  /**
   * A default value writer for integers
   */
  protected static final ValueWriter<Integer> INTEGER_VALUE_WRITER =
    new ValueWriter<Integer>() {
      @Override
      public void writeValue(DataOutput output, Integer value)
        throws IOException {

        output.writeInt(value);
      }
    };

  /**
   * A default value writer for longs
   */
  protected static final ValueWriter<Long> LONG_VALUE_WRITER =
    new ValueWriter<Long>() {
      @Override
      public void writeValue(DataOutput output, Long value) throws IOException {
        output.writeLong(value);
      }
    };

  protected <T extends Writable> ValueReader<T> getWritableValueReader(
    final Class<T> classLoader) {
    return new ValueReader<T>() {
      @Override
      public T readValue(DataInput input) throws IOException {
        T newInstance = null;

        try {
          newInstance = classLoader.newInstance();
        } catch (InstantiationException e) {
          throw new IOException(e);
        } catch (IllegalAccessException e) {
          throw new IOException(e);
        }

        newInstance.readFields(input);
        return newInstance;
      }
    };
  }

  protected <T extends Writable> ValueWriter<T> getWritableValueWriter() {
    return new ValueWriter<T>() {
      @Override
      public void writeValue(DataOutput output, T value) throws IOException {
        value.write(output);
      }
    };
  }

  /**
   * Functionality to parse a map out of the DataInput
   *
   * @param input       DataInput provided by {@code readFields}
   * @param map         The map structure in which the extracted values will be
   *                    inserted
   * @param keyReader   The ValueReader to read keys from the input
   * @param valueReader The ValueReader to read values from the input
   * @param <K>         The type of the Key of the map
   * @param <V>         The type of the Value of the map
   * @throws IOException Forwarded IOException from {@code input.readX()}
   */
  protected <K, V> void readMap(
    DataInput input, Map<K, V> map,
    ValueReader<K> keyReader, ValueReader<V> valueReader) throws IOException {

    int inputSize = input.readInt();

    for (int i = 0; i < inputSize; i++) {
      K key = keyReader.readValue(input);
      V value = valueReader.readValue(input);

      map.put(key, value);
    }
  }

  /**
   * Functionality to parse a collection out of the DataInput
   *
   * @param input       DataInput provided by {@code readFields}
   * @param collection  The collection structure in which the extracted values
   *                    will be inserted
   * @param valueReader The ValueReader to read items from the input
   * @param <T>         The type of the items in the collection
   * @throws IOException Forwarded IOException from {@code input.readX()}
   */
  protected <T> void readCollection(
    DataInput input, Collection<T> collection,
    ValueReader<T> valueReader) throws IOException {
    int inputSize = input.readInt();

    for (int i = 0; i < inputSize; i++) {
      T value = valueReader.readValue(input);

      collection.add(value);
    }
  }

  /**
   * Functionality to serialize a collection into the DataOutput
   *
   * @param output      DataOutput provided by {@code write}
   * @param collection  The collection which has to be serialized
   * @param valueWriter The ValueWriter to serialize each item
   * @param <T>         The type of the items in the collection
   * @throws IOException Forwarded IOException from {@code input.writeX()}
   */
  protected <T> void writeCollection(
    DataOutput output, Collection<T> collection,
    ValueWriter<T> valueWriter) throws IOException {
    output.writeInt(collection.size());

    for (T item : collection) {
      valueWriter.writeValue(output, item);
    }
  }


  /**
   * Functionality to serialize a map into the DataOutput
   *
   * @param output      DataOutput provided by {@code write}
   * @param map         The map which has to be serialized
   * @param keyWriter   The ValueWriter to serialize the keys into DataOutput
   * @param valueWriter The ValueWriter to serialize the values into DataOutput
   * @param <K>         The type of the Key of the map
   * @param <V>         The type of the Value of the map
   * @throws IOException Forwarded IOException from {@code input.writeX()}
   */
  protected <K, V> void writeMap(
    DataOutput output, Map<K, V> map,
    final ValueWriter<K> keyWriter, final ValueWriter<V> valueWriter)
    throws IOException {

    writeCollection(output, map.entrySet(), new ValueWriter<Map.Entry<K, V>>() {
      @Override
      public void writeValue(
        DataOutput output, Map.Entry<K, V> value) throws IOException {
        keyWriter.writeValue(output, value.getKey());
        valueWriter.writeValue(output, value.getValue());
      }
    });
  }

  /**
   * Interface for a Reader to read and parse simple or complex types from
   * DataInput
   *
   * @param <T> The type of the parsed and returned object
   */
  protected interface ValueReader<T> {
    /**
     * Reads, parses and returns the next value of Type T
     *
     * @param input DataInput provided by {@code readFields}
     * @return the next parsed value
     * @throws IOException Forwarded IOException from {@code input.readX()}
     *                     also could throw an exception when the next value
     *                     is of an invalid type
     */
    T readValue(DataInput input) throws IOException;
  }

  /**
   * Interface for a Writer to serialize and write simple or complex types
   * into DataOutput
   *
   * @param <T> The type of objects it supports to serialize and write
   */
  protected interface ValueWriter<T> {
    /**
     * Serializes and writes the value of Type T into DataOutput
     *
     * @param output DataOutput provided by {@code write}
     * @param value  The value which has to be serialized and stored
     * @throws IOException Forwarded IOException from {@code input.writeX()}
     */
    void writeValue(DataOutput output, T value) throws IOException;
  }
}
