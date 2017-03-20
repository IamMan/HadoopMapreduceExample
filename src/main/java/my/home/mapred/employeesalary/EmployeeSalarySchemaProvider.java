package my.home.mapred.employeesalary;

import my.home.mapred.parquet.SchemaProvider;
import org.apache.avro.Schema;

import java.io.IOException;


public class EmployeeSalarySchemaProvider implements SchemaProvider
{

  final static String EmployeeId = "employee_id";
  final static String DepartmentId = "department_id";
  final static String Salary = "salary";
  private final String schemaLocation = "/schemas/EmployeeSalaryScheme.json";
  private final Schema avroSchema = new Schema.Parser().parse(getClass().getResourceAsStream(schemaLocation));


  public EmployeeSalarySchemaProvider() throws IOException {}

  @Override
  public Schema getSchema()
  {
    return avroSchema;
  }

}
