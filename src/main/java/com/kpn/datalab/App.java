package com.kpn.datalab;

import example.avro.User;
import example.avro.User2;
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
 * Playing around with avro forward and backward compatibility
 *
 */
public class App 
{
    public static void print(String label, GenericRecord record) {
        System.out.println(String.format("%-20s %s", label, record));
    }
    public static void main( String[] args ) throws IOException {

        System.out.println("Investigating forward and backward Avro compatibility without embedded schemas");
        Schema user_schema = new Schema.Parser().parse(new File("src/main/avro/user.avsc"));
        Schema user2_schema = new Schema.Parser().parse(new File("src/main/avro/user2.avsc"));

        System.out.println("user_schema (user.avsc (User class))" + user_schema.toString(true));
        System.out.println("user2_schema (user2.avsc (User2 class))" + user2_schema.toString(true));

        User u1 = new User("Un", 1,"one");
        print("User u1",u1);
        AvroSerializer userSerializer = new AvroSerializer(user_schema);
        byte[] u1_bytes = userSerializer.serialize(u1);
        System.out.println(String.format("u1 serialized to hex %s", Hex.encodeHexString(u1_bytes)));

        GenericRecord u11 = userSerializer.deserialize(u1_bytes);
        print("GenericRecord u11",u11);

        User u12 = (User) userSerializer.deserialize(u1_bytes);
        print("User u12",u12);

        User2 u2 = new User2("Deux", "Dos", 2,"two");
        print("User2 u2",u2);

        AvroSerializer u2serializer = new AvroSerializer(user2_schema);
        byte[] u2_bytes = u2serializer.serialize(u2);
        System.out.println(String.format("u2 serialized to hex %s", Hex.encodeHexString(u2_bytes)));

        User2 u21 = (User2) u2serializer.deserialize(u2_bytes);
        print("User2 u21 ",u21);

        System.out.println("forward compatibility");
        System.out.println("   from serialized User "+ u1.toString());
        User2 u3 = (User2) (new AvroSerializer(user_schema, user2_schema)).deserialize(u1_bytes);
        print("User2 u3", u3);

        System.out.println("backward compatibility");
        System.out.println("   from serialized User2 "+ u2.toString());
        User u4 = (User) new AvroSerializer(user2_schema, user_schema).deserialize(u2_bytes);
        print("User u4", u4);

    }

    public static void test_read_generic(Schema schema, byte[] bytes) throws IOException {
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
    public static byte[] test_generic(Schema schema, AvroSerializer userSerializer) throws IOException {

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
        return userSerializer.serialize(user2);
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

    public static void test_user() throws IOException {
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
