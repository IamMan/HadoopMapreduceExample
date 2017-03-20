package my.home.mapred.parquet;


import my.home.mapred.employeesalary.EmployeeSalarySchemaProvider;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

public class ParquetUtils
{

  private static CompressionCodecName DEFAULT_PARQUET_COMPRESSION_CODEC = CompressionCodecName.SNAPPY;

  public static void generateEmployeeSalaryParquetFromCsvStream(InputStream csvData, FileSystem fs, Path outputPath)
      throws IOException
  {
    genericWriter(csvData, fs, outputPath, new EmployeeSalarySchemaProvider());
  }

  private static void genericWriter(InputStream csvData, FileSystem fs, Path outputPath, SchemaProvider schemaProvider)
      throws IOException
  {

    try (ParquetWriter<List<String>> writer = CsvParquetWriter.builder(outputPath)
                                                              .withSchema(schemaProvider.getSchema())
                                                              .withCompressionCodec(DEFAULT_PARQUET_COMPRESSION_CODEC)
                                                              .withConf(fs.getConf())
                                                              .build()
    ) {
      new BufferedReader(new InputStreamReader(csvData)).lines().forEach(tsvRow -> {
        try {
          if (!tsvRow.isEmpty()) {
            writer.write(Arrays.asList(tsvRow.split(",")));
          }
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }
}
