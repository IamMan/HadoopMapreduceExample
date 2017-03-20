package my.home.mapred.employeesalary;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class MaxSalaryMRWithTools extends Configured implements Tool
{

  private static final Log LOG = LogFactory.getLog(MaxSalaryMRWithTools.class);

  public int run(String[] args) throws Exception
  {
    if (args.length < 3) {
      LOG.info("maxsalary: <inDir> <tmpDir> <outDir>");
      return -1;
    }
    String inputPath = args[0];
    String tmpPath = args[1];
    String outPath = args[2];

    Job job[] = this.prepareJobs(inputPath, tmpPath, outPath, getConf());

    job[0].waitForCompletion(true);
    job[1].waitForCompletion(true);

    return 0;
  }

  private Job[] prepareJobs(String inputPath, String tmpPath, String outPath, Configuration conf) throws IOException
  {
    return prepareJobs(new Path(inputPath), new Path(tmpPath), new Path(outPath), conf);
  }

  public Job[] prepareJobs(Path inputPath, Path tmpPath, Path outPath, Configuration conf) throws IOException
  {
    return new Job[]{
        prepareFirstJob(inputPath, tmpPath, conf),
        prepareSecondJob(tmpPath, outPath, conf)
    };
  }

  private Job prepareFirstJob(Path inputPath, Path outPath, Configuration conf) throws IOException
  {
    Job job = Job.getInstance(conf, "max salary prepare");
    job.setJarByClass(MaxSalaryMRWithTools.class);


    job.setInputFormatClass(AvroParquetInputFormat.class);
    AvroParquetInputFormat.addInputPath(job, inputPath);
    job.setMapperClass(DepartmentSalaryMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(DepartmentAndSalary.class);

    job.setCombinerClass(MaxSalarySingleDepartmentCombiner.class);

    job.setReducerClass(MaxSalaryAvroOutputReducer.class);
    job.setOutputKeyClass(Void.class);
    job.setOutputValueClass(GenericRecord.class);
    job.setOutputFormatClass(AvroParquetOutputFormat.class);
    AvroParquetOutputFormat.setSchema(job, MaxSalaryAvroOutputReducer.getSchema());
    AvroParquetOutputFormat.setOutputPath(job, outPath);

    return job;
  }

  private Job prepareSecondJob(Path inputPath, Path outPath, Configuration conf) throws IOException
  {
    Job job = Job.getInstance(conf, "max salary finish");
    job.setJarByClass(MaxSalaryMRWithTools.class);

    job.setInputFormatClass(AvroParquetInputFormat.class);
    AvroParquetInputFormat.addInputPath(job, inputPath);
    job.setMapperClass(TotalMaxSalaryMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(TotalMaxSalaryReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outPath);

    return job;
  }

  // This is the custom for managing intermediate results.
  // Of course we can use GenericRecord instead, this can be more general solution, but not so convenient
  public static class DepartmentAndSalary implements Writable
  {
    private int employee;
    private int department;
    private int salary;

    DepartmentAndSalary(int employee, int department, int salary)
    {
      this.employee = employee;
      this.department = department;
      this.salary = salary;
    }

    DepartmentAndSalary()
    {
      this.employee = 0;
      this.department = 0;
      this.salary = 0;
    }

    public void write(DataOutput out) throws IOException
    {
      out.writeInt(employee);
      out.writeInt(department);
      out.writeInt(salary);
    }

    public void readFields(DataInput in) throws IOException
    {
      employee = in.readInt();
      department = in.readInt();
      salary = in.readInt();
    }

    int getEmployee()
    {
      return employee;
    }

    int getDepartment()
    {
      return department;
    }

    int getSalary()
    {
      return salary;
    }
  }

  // Mapper for select EmployeeId, DepartmentId and Salary. Output: (EmployeeId, DepartmentAndSalary)
  public static class DepartmentSalaryMapper
      extends Mapper<LongWritable, GenericRecord, IntWritable, DepartmentAndSalary>
  {
    @Override
    protected void map(LongWritable key, GenericRecord value, Context context) throws IOException, InterruptedException
    {
      Integer employee = (Integer) value.get(EmployeeSalarySchemaProvider.EmployeeId);
      Integer department = (Integer) value.get(EmployeeSalarySchemaProvider.DepartmentId);
      Integer salary = (Integer) value.get(EmployeeSalarySchemaProvider.Salary);
      context.write(
          new IntWritable(employee),
          new DepartmentAndSalary(employee, department, salary)
      );
    }
  }

  // Combiner for reduce mapped values
  public static class MaxSalarySingleDepartmentCombiner
      extends Reducer<IntWritable, DepartmentAndSalary, IntWritable, DepartmentAndSalary>
  {

    static DepartmentAndSalary SelectMaxWhenDepartmentUnique(
        IntWritable employeeId,
        Iterable<DepartmentAndSalary> values
    )
    {
      Integer department = null;
      DepartmentAndSalary maxDepAndSalary = new DepartmentAndSalary();
      for (DepartmentAndSalary depAndSalary : values) {
        if (department == null) {
          department = depAndSalary.getDepartment();
        }
        if (department != depAndSalary.getDepartment()) {
          return null;
        }
        if (depAndSalary.getSalary() > maxDepAndSalary.getSalary()) {
          maxDepAndSalary.employee = depAndSalary.employee;
          maxDepAndSalary.salary = depAndSalary.salary;
          maxDepAndSalary.department = depAndSalary.department;
        }
      }
      return maxDepAndSalary;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<DepartmentAndSalary> values, Context context)
        throws IOException, InterruptedException
    {
      DepartmentAndSalary maxDepAndSalary = SelectMaxWhenDepartmentUnique(key, values);
      if (maxDepAndSalary != null) {
        context.write(key, maxDepAndSalary);
      }
    }
  }

  //Avro output reducer. Output: (GenericRecord(EmployeeId, Salary)). In this step we can leave only Salary, but why not?
  public static class MaxSalaryAvroOutputReducer extends Reducer<IntWritable, DepartmentAndSalary, Void, GenericRecord>
  {
    private static Schema maxSalaryAvroScheme = SchemaBuilder
        .record("MaxSalaryByEmployee")
        .fields()
        .name(EmployeeSalarySchemaProvider.EmployeeId).type().intType().noDefault()
        .name(EmployeeSalarySchemaProvider.Salary).type().intType().noDefault()
        .endRecord();

    static Schema getSchema()
    {
      return maxSalaryAvroScheme;
    }

    static GenericRecord buildParquetRecord(DepartmentAndSalary obj)
    {
      return new GenericRecordBuilder(getSchema())
          .set("employee_id", obj.getEmployee())
          .set("salary", obj.getSalary())
          .build();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<DepartmentAndSalary> values, Context context)
        throws IOException, InterruptedException
    {
      DepartmentAndSalary maxSalary = MaxSalarySingleDepartmentCombiner.SelectMaxWhenDepartmentUnique(key, values);
      if (maxSalary != null) {
        GenericRecord genericRecord = buildParquetRecord(maxSalary);
        context.write(null, genericRecord);
      }
    }
  }

  //TotalMaxSalaryMapper. OutPut: (0, Salary). Zero key is for use only one reducer
  public static class TotalMaxSalaryMapper extends Mapper<LongWritable, GenericRecord, IntWritable, IntWritable>
  {
    @Override
    protected void map(LongWritable key, GenericRecord value, Context context) throws IOException, InterruptedException
    {
      Integer salary = (Integer) value.get(EmployeeSalarySchemaProvider.Salary);
      context.write(
          new IntWritable(0),
          new IntWritable(salary)
      );
    }
  }

  //TotalMaxSalaryReducer. Just calc max salary on one reducer
  public static class TotalMaxSalaryReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable>
  {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException
    {
      IntWritable maxSalary = new IntWritable(0);
      for (IntWritable value : values) {
        if (value.compareTo(maxSalary) > 0) {
          maxSalary = new IntWritable(value.get());
        }
      }
      context.write(new Text("MaxSalary"), maxSalary);
    }
  }
}
