package my.home.mapred.parquet;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;


public class CsvParquetWriter extends ParquetWriter<List<String>>
{

  public CsvParquetWriter(Path file, MessageType schema) throws IOException
  {
    this(file, schema, false);
  }

  private CsvParquetWriter(Path file, MessageType schema, boolean enableDictionary) throws IOException
  {
    this(file, schema, CompressionCodecName.UNCOMPRESSED, enableDictionary);
  }

  private CsvParquetWriter(Path file, MessageType schema, CompressionCodecName codecName, boolean enableDictionary)
      throws IOException
  {
    super(file, new CsvWriteSupport(schema), codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, false);
  }

  static CsvParquetWriter.Builder builder(Path file)
  {
    return new CsvParquetWriter.Builder(file);
  }

  public static class Builder
      extends org.apache.parquet.hadoop.ParquetWriter.Builder<List<String>, CsvParquetWriter.Builder>
  {
    private MessageType messageType;

    private Builder(Path file)
    {
      super(file);
      this.messageType = null;
    }

    CsvParquetWriter.Builder withSchema(Schema schema)
    {
      this.messageType = new AvroSchemaConverter().convert(schema);
      return this;
    }

    public CsvParquetWriter.Builder withMessageType(MessageType messageType)
    {
      this.messageType = messageType;
      return this;
    }

    protected CsvParquetWriter.Builder self()
    {
      return this;
    }

    protected WriteSupport<List<String>> getWriteSupport(Configuration conf)
    {
      return new CsvWriteSupport(this.messageType);
    }
  }


}
