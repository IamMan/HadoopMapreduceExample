package my.home.mapred;

import my.home.mapred.employeesalary.MaxSalaryMRWithTools;
import my.home.mapred.parquet.ParquetUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class MaxSalaryClusterTest
{

  private static final Log LOG = LogFactory.getLog(MaxSalaryClusterTest.class);
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");
  private static final String warehouseDir = "test-warehouse";
  private static final String employeeSalaryData = "employee-salary";
  private static final Path employeeSalaryDataPath = new Path(warehouseDir, employeeSalaryData);
  private static final String resultsDir = "test-results";
  private static final Path resultsPath = new Path(warehouseDir, resultsDir);
  private static final String testGroupName = "maxsalary";
  private static final String EXPECTED_RESULTS_SUFFIX = "_results.txt";
  private static final String RESULTS_NAMED_OUTPUT_PREFIX = "part-r-";
  private static MiniDFSCluster mrCluster;
  private static Configuration fsConfig = null;
  private static FileSystem fs = null;

  @BeforeClass
  public static void setup() throws IOException
  {

    // create the mini cluster to be used for the tests
    System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
    fsConfig = new HdfsConfiguration();

    File testDataDir = new File(warehouseDir);
    String testDataStr = testDataDir.getAbsolutePath();
    fsConfig.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDataStr);

    mrCluster = new MiniDFSCluster.Builder(fsConfig).build();
    fs = FileSystem.get(fsConfig);

  }

  @AfterClass
  public static void cleanup() throws IOException
  {
    // stopping the mini cluster
    mrCluster.shutdown();

    File testDataDir = new File(warehouseDir);
    FileUtils.deleteDirectory(testDataDir);
  }

  @Test
  public void testMaxSalarySimple() throws Exception
  {
    final String testName = "simple";
    final String outTestDirName = "max-salary-results";

    final Path jobsInputPath = employeeSalaryDataPath;
    final Path jobsTmpPath = new Path(resultsPath, new Path("tmp"));
    final Path jobsOutputPath = new Path(resultsPath, new Path(outTestDirName));

    //prepare test data
    InputStream testData = getDataFromResources(employeeSalaryData, testName + ".csv");
    ParquetUtils.generateEmployeeSalaryParquetFromCsvStream(testData, fs, jobsInputPath);

    // clean tmp and output dir
    cleanOutputDirs(jobsTmpPath, jobsOutputPath);

    //Prepare jobs
    fsConfig.set("mapred.textoutputformat.separator", " ");
    MaxSalaryMRWithTools maxSalaryProvider = new MaxSalaryMRWithTools();
    Job[] jobs = maxSalaryProvider.prepareJobs(jobsInputPath, jobsTmpPath, jobsOutputPath, fsConfig);

    //Wait while all job finished SUCCESSFUL
    waitJobFinished(jobs);

    // Make sure that results correct
    String results = downloadTotalResults(jobsOutputPath);
    String expectedResults = StringUtils.joinString(getDataFromResources(
        testGroupName,
        testName + EXPECTED_RESULTS_SUFFIX
    ), LINE_SEPARATOR);
    assertEquals("SimpleTest output should be equal to expected", expectedResults, results);
  }

  private InputStream getDataFromResources(String dataSource, String name)
  {
    return this.getClass().getResourceAsStream("/" + dataSource + "/" + name);
  }

  private void cleanOutputDirs(Path jobsTmpPath, Path jobsOutputPath) throws IOException
  {
    File outDir = new File(jobsOutputPath.toString());
    FileUtils.deleteDirectory(outDir);
    File tmpDir = new File(jobsTmpPath.toString());
    FileUtils.deleteDirectory(tmpDir);
  }

  private void waitJobFinished(Job[] jobs) throws InterruptedException, IOException, ClassNotFoundException
  {
    int index = 1;
    for (Job job : jobs) {
      job.waitForCompletion(true);
      assertTrue(String.format("Job %d should finish successful", index), job.isSuccessful());
      index++;
    }
  }

  private String downloadTotalResults(Path resultsPath) throws IOException
  {
    FileStatus[] fileStatuses = fs.globStatus(new Path(resultsPath, new Path(RESULTS_NAMED_OUTPUT_PREFIX + "*")));
    StringBuilder result = new StringBuilder();
    for (FileStatus fileStatus : fileStatuses) {
      result.append(StringUtils.joinString(fs.open(fileStatus.getPath()), LINE_SEPARATOR));
      result.append(LINE_SEPARATOR);
    }
    result.deleteCharAt(result.length() - 1);
    return result.toString();
  }
}
