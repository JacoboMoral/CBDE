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
import org.apache.hadoop.hbase.KeyValue;
// Hadoop classes
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Selection extends Configured implements Tool {
	private static String inputTable;
	private static String outputTable;




	//=================================================================== Main

	public static void main(String[] args) throws Exception {
		if (args.length<4) {
			System.err.println("Parameters missing: 'inputTable outputTable [family:]attribute value'");
			System.exit(1);
		}
		inputTable = args[0];
		outputTable = args[1];

		int tablesRight = checkIOTables(args);
		if (tablesRight==0) {
			int ret = ToolRunner.run(new Selection(), args);
			System.exit(ret);
		}
		else System.exit(tablesRight);
	}


	//============================================================== checkTables
	private static int checkIOTables(String [] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin hba = new HBaseAdmin(config);

		if (!hba.tableExists(inputTable)) {
			System.err.println("Input table does not exist");
			return 2;
		}
		if (hba.tableExists(outputTable)) {
			System.err.println("Output table already exists");
			return 3;
		}

		HTableDescriptor htdInput = hba.getTableDescriptor(inputTable.getBytes());
		HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());

		for (byte[] famkey: htdInput.getFamiliesKeys()) {
			htdOutput.addFamily(new HColumnDescriptor(famkey));
		}
		hba.createTable(htdOutput);
		return 0;
	}

	//============================================================== Job config
	public int run(String [] args) throws Exception {

		Job job = new Job(HBaseConfiguration.create());
		job.setJarByClass(Selection.class);
		job.setJobName("Selection");
		Scan scan = new Scan();

		String header = args[2] + "," + args[3];	//[family:]attribute,value

		job.getConfiguration().setStrings("attributes",header);
		TableMapReduceUtil.initTableMapperJob(inputTable, scan, Mapper.class, Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 4;
	}


	//=================================================================== Mapper
	public static class Mapper extends TableMapper<Text, Text> {

		public void map(ImmutableBytesWritable rowMetadata, Result values, Context context) throws IOException, InterruptedException {
			String rowId = new String(rowMetadata.get(), "US-ASCII"); //key
			String[] attributes = context.getConfiguration().getStrings("attributes","empty");
			String attribute;
			KeyValue[] attributesRaw;
			String tuple;
			String[] firstFamilyColumn = new String[2];

			if (!attributes[0].contains(":")){
				firstFamilyColumn[0] =  attributes[0];
				firstFamilyColumn[1] =  attributes[0];
				//we assume that both family and column name are the same if only one name is provided (a -> a:a)
			}
			else firstFamilyColumn = attributes[0].split(":");

			attribute = new String(values.getValue(firstFamilyColumn[0].getBytes(),firstFamilyColumn[1].getBytes()));

			if (attribute.equals(attributes[1])){ //input x:x == row y:y
				attributesRaw = values.raw();
				tuple = new String(attributesRaw[0].getFamily()) + ":" + new String(attributesRaw[0].getQualifier()) + ":" + new String(attributesRaw[0].getValue());
				for (int i=1;i<attributesRaw.length;i++) {
					tuple += ";" + new String(attributesRaw[i].getFamily()) + ":" + new String(attributesRaw[i].getQualifier()) + ":" + new String(attributesRaw[i].getValue());
				}
				context.write(new Text(rowId), new Text(tuple));
			}
		}
	}

	//================================================================== Reducer
	public static class Reducer extends TableReducer<Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {

			while (inputList.iterator().hasNext()){
				Text outputKey = inputList.iterator().next();
				Put put = new Put(key.getBytes());

				for (String tuple : outputKey.toString().split(";")){
					String[] values = tuple.split(":");
					put.add(values[0].getBytes(),values[1].getBytes(),values[2].getBytes());
				}
				context.write(outputKey, put);
			}
		}
	}
}
