
package com.kpn.datalab;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 *
 * @author pmaresca
 */
public class AvroSerializer 
{
    private static AvroSerializer instance;
    
    private boolean inited;
    
    private DatumReader<GenericRecord> reader;
    private DatumWriter<GenericRecord> writer;
    
    private ByteArrayOutputStream baos;

    public AvroSerializer(Schema reader_writer) {
        this(reader_writer, null);
    }

    public AvroSerializer(Schema writer, Schema reader) {
        if (reader != null)
            // writer reader
            this.reader = new SpecificDatumReader<>(writer, reader);
        else
            this.reader = new SpecificDatumReader<>(writer);
        this.writer = new SpecificDatumWriter<>(writer);
        this.baos = new ByteArrayOutputStream();
        this.inited = true;
    }
    
    public byte[] serialize(Object msg) throws IOException, IllegalStateException
    {
        this.baos.reset();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(this.baos, null);
        
        
        writer.write((GenericRecord)msg, encoder);
        encoder.flush();
        this.baos.flush();
        
        return this.baos.toByteArray();
    }
    
    public GenericRecord deserialize(byte[] msg) throws IOException, IllegalStateException
    { 
        if(!inited)
            throw new IllegalStateException("Serializer must be initialized");
        
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(msg, null);
        
        return reader.read(null, decoder);
    }
    
    public void tearDown() throws IOException
    {
        this.reader = null;
        this.writer = null;
        this.baos.close();
        this.inited = false;
    }
    
}
