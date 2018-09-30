// Standard classes
import java.io.IOException;
import java.util.Vector;
import java.util.ArrayList;

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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.TableName;

// Hadoop classes
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Join extends Configured implements Tool {

    // These are three global variables, which coincide with the three parameters in the call
    public static String inputTable1;
    public static String inputTable2;
    private static String outputTable;


    public static void main(String[] args) throws Exception {
        if (args.length<5) {
            System.err.println("Parameters missing: 'inputTableEXT inputTableINT outputTable inputAttributeEXT inputAttributeINT'");
            System.exit(1);
        }
        inputTable1 = args[0];
        inputTable2 = args[1];
        outputTable = args[2];



        int tablesRight = checkIOTables(args);
        if (tablesRight==0) {
            int ret = ToolRunner.run(new Join(), args);
            System.exit(ret);
        } else {
            System.exit(tablesRight);
        }
    }


    //============================================================== checkTables

    private static int checkIOTables(String [] args) throws Exception {

        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hba = new HBaseAdmin(config);

        if (!hba.tableExists(inputTable1)) {
            System.err.println("Input table 1 does not exist");
            return 2;
        }
        if (!hba.tableExists(inputTable2)) {
            System.err.println("Input table 2 does not exist");
            return 2;
        }
        if (hba.tableExists(outputTable)) {
            System.err.println("Output table already exists");
            return 3;
        }

        HTableDescriptor htdInput1 = hba.getTableDescriptor(inputTable1.getBytes());
        HTableDescriptor htdInput2 = hba.getTableDescriptor(inputTable2.getBytes());

        HTableDescriptor htdOutput = new HTableDescriptor(outputTable.getBytes());

        for(byte[] key: htdInput1.getFamiliesKeys()) {
            System.out.println("family-t1 = "+ new String(key));
            htdOutput.addFamily(new HColumnDescriptor(key));
        }
        for(byte[] key: htdInput2.getFamiliesKeys()) {
            System.out.println("family-t2 = "+ new String(key));
            htdOutput.addFamily(new HColumnDescriptor(key));
        }
        hba.createTable(htdOutput);
        return 0;
    }


    //============================================================== Job config
    public int run(String [] args) throws Exception {

        Job job = new Job(HBaseConfiguration.create());
        job.setJarByClass(Join.class);
        job.setJobName("Join");
        job.getConfiguration().setStrings("External", inputTable1);
        job.getConfiguration().setStrings("Internal", inputTable2);
        job.getConfiguration().setStrings("externalAttribute", args[3]);
        job.getConfiguration().setStrings("internalAttribute", args[4]);


        ArrayList scans = new ArrayList();

        Scan scan1 = new Scan();
        System.out.println("inputTable1: "+inputTable1);

        scan1.setAttribute("scan.attributes.table.name", Bytes.toBytes(inputTable1));
        scans.add(scan1);

        Scan scan2 = new Scan();
        System.err.println("inputTable2: "+inputTable2);
        scan2.setAttribute("scan.attributes.table.name", Bytes.toBytes(inputTable2));

        scans.add(scan2);

        TableMapReduceUtil.initTableMapperJob(scans, Mapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(outputTable, Reducer.class, job);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }


    //=================================================================== Mapper

    public static class Mapper extends TableMapper<Text, Text> {

        public void map(ImmutableBytesWritable rowMetadata, Result values, Context context) throws IOException, InterruptedException {
            int i;

            String[] external = context.getConfiguration().getStrings("External", "Default");
            String[] internal = context.getConfiguration().getStrings("Internal", "Default");
            String[] externalAttribute = context.getConfiguration().getStrings("externalAttribute", "Default");
            String[] internalAttribute = context.getConfiguration().getStrings("internalAttribute", "Default");

            TableSplit currentSplit = (TableSplit)context.getInputSplit();

            TableName tableNameB = currentSplit.getTable();
            String tableName = tableNameB.getQualifierAsString();

            String tuple = tableName+"#"+new String(rowMetadata.get(), "US-ASCII");

            KeyValue[] attributes = values.raw();



            if (tableName.equalsIgnoreCase(external[0])) {
                String qualif = null;
                String value = null;
                for (i=0;i<attributes.length;i++){
                    qualif = new String(attributes[i].getQualifier());
                    if (qualif.equals(externalAttribute[0])) value = new String(attributes[i].getValue());
                    tuple = tuple+";"+new String(attributes[i].getFamily())+":"+new String(attributes[i].getQualifier())+":"+new String(attributes[i].getValue());
                }
                if (value != null) context.write(new Text(value), new Text(tuple));
            }

            if (tableName.equalsIgnoreCase(internal[0])) {
                String qualif = null;
                String value = null;
                for (i=0;i<attributes.length;i++){
                    qualif = new String(attributes[i].getQualifier());
                    if (qualif.equals(internalAttribute[0])) value = new String(attributes[i].getValue());
                    tuple = tuple+";"+new String(attributes[i].getFamily())+":"+new String(attributes[i].getQualifier())+":"+new String(attributes[i].getValue());
                }
                if (value != null) context.write(new Text(value), new Text(tuple));
            }
        }
    }

    //================================================================== Reducer
    public static class Reducer extends TableReducer<Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> inputList, Context context) throws IOException, InterruptedException {
            int i,j,k;
            Put put;
            String eTableTuple, iTableTuple;
            String eTuple, iTuple;
            String outputKey;
            String[] external = context.getConfiguration().getStrings("External","Default");
            String[] internal = context.getConfiguration().getStrings("Internal","Default");
            String[] eAttributes, iAttributes;
            String[] attribute_value;
            String[] externalAttribute = context.getConfiguration().getStrings("externalAttribute", "Default");
            String[] internalAttribute = context.getConfiguration().getStrings("internalAttribute", "Default");

            Vector<String> tuples = new Vector<String>();
            for (Text val : inputList) {
                tuples.add(val.toString());
            }
            for (i=0;i<tuples.size();i++) {
                eTableTuple = tuples.get(i);
                eTuple=eTableTuple.split("#")[1];
                eAttributes=eTuple.split(";");
                if (eTableTuple.startsWith(external[0])) {
                    for (j=0;j<tuples.size();j++) {
                        iTableTuple = tuples.get(j);
                        iTuple=iTableTuple.split("#")[1];
                        iAttributes=iTuple.split(";");
                        if (iTableTuple.startsWith(internal[0])) {
                            outputKey = eAttributes[0]+"_"+iAttributes[0];
                            put = new Put(outputKey.getBytes());
                            String externalTableValue = null;
                            String internalTableValue = null;

                            for (k=1;k<eAttributes.length;k++) {
                                attribute_value = eAttributes[k].split(":");
                                if(attribute_value[1].equals(externalAttribute[0])) externalTableValue = attribute_value[2];
                                put.addColumn(attribute_value[0].getBytes(), attribute_value[1].getBytes(), attribute_value[2].getBytes());
                            }

                            for (k=1;k<iAttributes.length;k++) {
                                attribute_value = iAttributes[k].split(":");
                                if(attribute_value[1].equals(internalAttribute[0])) internalTableValue = attribute_value[2];
                                put.addColumn(attribute_value[0].getBytes(), attribute_value[1].getBytes(), attribute_value[2].getBytes());
                            }

                            if(externalTableValue!=null && internalTableValue!=null && externalTableValue.equals(internalTableValue)) context.write(new Text(outputKey), put);
                        }
                    }
                }
            }
        }
    }
}
