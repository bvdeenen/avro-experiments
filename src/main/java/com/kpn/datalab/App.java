package com.kpn.datalab;

import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.binary.Hex;


import java.io.File;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, NoSuchMethodException, NoSuchFieldException {
        //test_function_generation();

        Schema schema = new Schema.Parser().parse(new File("src/main/avro/user.avsc"));
        AvroSerializer.instance().setUp(schema);
        byte[] bytes = test_nocode(schema);
        test_read_nocode_no_schema(schema, bytes);
    }

    public static void test_read_nocode_no_schema(Schema schema, byte[] bytes) throws IOException {
        // Deserialize users from disk
        System.out.println(Hex.encodeHexString(bytes));
        User u = (User) AvroSerializer.instance().deserialize(bytes);
            System.out.println(u);

            GenericRecord u2 = AvroSerializer.instance().deserialize(bytes);
            System.out.println(u2);

    }
    public static void test_read_nocode(Schema schema, byte[] bytes) throws IOException {
        // Deserialize users from disk
        File file = new File("users2.avro-noschema");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
// Reuse user object by passing it to next(). This saves us from
// allocating and garbage collecting many objects for files with
// many items.
            user = dataFileReader.next(user);
            System.out.println(user);

        }
    }
    public static byte[] test_nocode(Schema schema) throws IOException, NoSuchMethodException, NoSuchFieldException {

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);
// Leave favorite color null

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");
        //writeGenericRecord(schema, user1, user2);
        User u = new User("Maja", 1,"rood");
        return AvroSerializer.instance().serialize(user2);
    }

    public static void writeGenericRecord(Schema schema, GenericRecord user1, GenericRecord user2) throws IOException {
        // Serialize user1 and user2 to disk
        File file = new File("users2.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
    }

    public static void test_function_generation() throws IOException {
        User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(1234);
        User user2 = new User("Ben", 7, "red");
        User user3 = User.newBuilder()
                .setName("Bart")
                .setFavoriteColor("yellow")
                .setFavoriteNumber(31415)
                .build();
// Serialize user1, user2 and user3 to disk
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File("users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();

        File file = new File("users.avro");
// Deserialize Users from disk
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
// Reuse user object by passing it to next(). This saves us from
// allocating and garbage collecting many objects for files with
// many items.
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }
}
