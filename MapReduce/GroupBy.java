// Standard classes
import java.io.IOException;
// HBase classes
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Result;
// Hadoop classes
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class GroupBy extends Configured implements Tool {
    private static String inputTable;
    private static String outputTable;



    //=================================================================== Main


    public static void main(String[] args) throws Exception {
        if (args.length<4) {
            System.err.println("Parameters missing: 'inputTable outputTable aggregatefamily:attribute groupbyfamily:attribute'");
            System.exit(1);
        }
        inputTable = args[0];
        outputTable = args[1];

        int tablesRight = checkIOTables(args);
        if (tablesRight==0) {
            int ret = ToolRunner.run(new GroupBy(), args);
            System.exit(ret);
        } else {
            System.exit(tablesRight);
        }
    }


    //============================================================== checkTables
    private static int checkIOTables(String [] args) throws Exception {
        // Obtain HBase's configuration
        Configuration config = HBaseConfiguration.create();
        // Create an HBase administrator
        HBaseAdmin hba = new HBaseAdmin(config);

        // With an HBase administrator we check if the input table exists
        if (!hba.tableExists(inputTable)) {
            System.err.println("Input table does not exist");
            return 2;
        }
        // Check if the output table exists
        if (hba.tableExists(outputTable)) {
            System.err.println("Output table already exists");
            return 3;
        }
        // Create the columns of the output table
        HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());
        //Add columns to the new table

        String[] familyColumn = new String[2];
        familyColumn = args[2].split(":");
        htdOutput.addFamily(new HColumnDescriptor(familyColumn[0]));


        hba.createTable(htdOutput);
        return 0;
    }

    //============================================================== Job config
    public int run(String [] args) throws Exception {
        //Create a new job to execute

        //Retrive the configuration
        Job job = new Job(HBaseConfiguration.create());
        //Set the MapReduce class
        job.setJarByClass(GroupBy.class);
        //Set the job name
        job.setJobName("GroupBy");
        //Create an scan object
        Scan scan = new Scan();
        //Set the columns to scan and keep header to project

        String header = args[2]+','+args[3];
        job.getConfiguration().setStrings("attributes",header);
        //Set the Map and Reduce function
        TableMapReduceUtil.initTableMapperJob(inputTable, scan, Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 4;
    }


    //=================================================================== Mapper
    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable rowMetadata, Result values, Context context) throws IOException, InterruptedException {
            String[] attributes = context.getConfiguration().getStrings("attributes","empty");

            String[] aggregation=attributes[0].split(":");
            String[] groupBy=attributes[1].split(":");

            String value = new String(values.getValue(aggregation[0].getBytes(),aggregation[1].getBytes()));
            String key = new String(values.getValue(groupBy[0].getBytes(),groupBy[1].getBytes()));

            context.write(new Text(key), new Text(value));
        }
    }

    //================================================================== Reducer
    public static class Reducer extends TableReducer<Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {
            String[] attributes = context.getConfiguration().getStrings("attributes","empty");

            String family = attributes[0].split(":")[0];
            String attribute = attributes[0].split(":")[1]; //family:attribute of aggregate

            int sum = 0;
            while(inputList.iterator().hasNext()){
                Text valueArray = inputList.iterator().next();
                sum += Integer.parseInt(valueArray.toString());
            }


            Put put = new Put(key.getBytes());
            put.add(family.getBytes(),attribute.getBytes(),Integer.toString(sum).getBytes());

            context.write(new Text(key), put);
        }

    }
}
