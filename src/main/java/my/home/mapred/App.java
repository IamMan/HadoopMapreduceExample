package my.home.mapred;

import my.home.mapred.employeesalary.MaxSalaryMRWithTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;


public class App
{
  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: maxsalary <in> <out>");
      System.exit(2);
    }

    Job[] jobs = (new MaxSalaryMRWithTools()).prepareJobs(
        new Path(otherArgs[0]),
        new Path(otherArgs[1]),
        new Path(otherArgs[2]),
        new Configuration()
    );
    if (jobs[0].waitForCompletion(true)) {
      System.exit(jobs[0].waitForCompletion(true) ? 0 : 1);
    } else {
      System.exit(1);
    }
  }
}
