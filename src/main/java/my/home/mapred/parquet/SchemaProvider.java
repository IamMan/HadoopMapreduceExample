package my.home.mapred.parquet;

import org.apache.avro.Schema;

public interface SchemaProvider
{
  Schema getSchema();
}
