/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients; 
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;



 import com.mongodb.client.model.Indexes;
 import com.mongodb.client.model.IndexOptions;
 import com.mongodb.client.model.Filters;


 import com.mongodb.client.model.Aggregates;
 import com.mongodb.client.model.Accumulators;
 import com.mongodb.client.model.Projections;
     

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.Arrays;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
public class Lab4 {

    public static MongoCollection<Document> query1;
    public static MongoCollection<Document> query2;
    public static MongoCollection<Document> query34;
    
    //constantes
    public static int date = 15;
    public static int dateYear = 40;
    public static int size = 3;
    public static String type = ".*type";
    public static String region = "rname1";
    public static String segment = "mkt1";
    public static int date1 = 16;
    public static int date2 = 14;
    
    
    public static void main(String[] args) {
        try (MongoClient client = new MongoClient("localhost", 27017)) {
            client.dropDatabase("mydb");
            MongoDatabase database = client.getDatabase("mydb");
            query1 = database.getCollection("query1");
            query2 = database.getCollection("query2");
            query34 = database.getCollection("query34");
            Block<Document> processBlock = new Block<Document>() {
                @Override
                public void apply(final Document document) {
                    System.out.println(document);
                }
            };     
        //inserts 
        insertQuery1();
        insertQuery34();
        insertQuery2();
        //aggregation
        List<? extends Bson> pipeline1 = aggregationQuery1();
        List<? extends Bson> pipeline2 = aggregationQuery2();
        List<? extends Bson> pipeline3 = aggregationQuery3();
        List<? extends Bson> pipeline4 = aggregationQuery4();
        //QUERY 1
        System.out.println("============ QUERY 1 ============");
        query1.aggregate(pipeline1)
                .allowDiskUse(false)
                .forEach(processBlock); 
        
        
        //QUERY 2
        System.out.println("============ QUERY 2 ============");
        query2.aggregate(pipeline2)
                .allowDiskUse(false)
                .forEach(processBlock);
        //QUERY 3
        System.out.println("============ QUERY 3 ============");
        query34.aggregate(pipeline3)
                .allowDiskUse(false)
                .forEach(processBlock);
        //QUERY 4
        System.out.println("============ QUERY 4 ============");
        query34.aggregate(pipeline4)
                .allowDiskUse(false)
                .forEach(processBlock);
        

        
    } catch (MongoException e) {
            // handle MongoDB exception
    }
        
    }

    private static List<? extends Bson>  aggregationQuery1() {
        List<? extends Bson> pipeline = Arrays.asList(
                    new Document()
                            .append("$match", new Document()
                                    .append("l_shipdate", new Document()
                                            .append("$lte", date)
                                    )
                            ), 
                    new Document()
                            .append("$group", new Document()
                                    .append("_id", new Document()
                                            .append("l_returnflag", "$l_returnflag")
                                            .append("l_linestatus", "$l_linestatus")
                                    )
                                    .append("sum_qty", new Document()
                                            .append("$sum", "$l_quantity")
                                    )
                                    .append("sum_base_price", new Document()
                                            .append("$sum", "$l_extendedprice")
                                    )
                                    .append("sum_disc_price", new Document()
                                            .append("$sum", new Document()
                                                    .append("$multiply", Arrays.asList(
                                                            "$l_extendedprice",
                                                            new Document()
                                                                    .append("$subtract", Arrays.asList(
                                                                            1.0,
                                                                            "$l_discount"
                                                                        )
                                                                    )
                                                        )
                                                    )
                                            )
                                    )
                                    .append("sum_charge", new Document()
                                            .append("$sum", new Document()
                                                    .append("$multiply", Arrays.asList(
                                                            "$l_extendedprice",
                                                            new Document()
                                                                    .append("$subtract", Arrays.asList(
                                                                            1.0,
                                                                            "$l_discount"
                                                                        )
                                                                    ),
                                                            new Document()
                                                                    .append("$add", Arrays.asList(
                                                                            1.0,
                                                                            "$l_tax"
                                                                        )
                                                                    )
                                                        )
                                                    )
                                            )
                                    )
                                    .append("avg_qty", new Document()
                                            .append("$avg", "$l_quantity")
                                    )
                                    .append("avg_price", new Document()
                                            .append("$avg", "$l_extendedprice")
                                    )
                                    .append("avg_disc", new Document()
                                            .append("$avg", "$l_discount")
                                    )
                                    .append("count_order", new Document()
                                            .append("$sum", 1.0)
                                    )
                            ), 
                    new Document()
                            .append("$sort", new Document()
                                    .append("l_returnflag", 1.0)
                                    .append("l_linestatus", 1.0)
                            )
            );
        return pipeline;
    }
    private static List<? extends Bson> aggregationQuery2() {
         List<? extends Bson> pipeline = Arrays.asList(
                    new Document()
                            .append("$match", new Document()
                                    .append("r_name", region)
                            ), 
                    new Document()
                            .append("$group", new Document()
                                    .append("_id", "costeMinimo")
                                    .append("minpscost", new Document()
                                            .append("$min", "$ps_supplycost")
                                    )
                            )
            );
         Integer mincost = -3;
         try {
            mincost = (Integer) (query2.aggregate(pipeline)
                .allowDiskUse(false)
                 .first().get("minpscost"));
        } catch (Exception e) {
        }
         
         pipeline = Arrays.asList(
                    new Document()
                            .append("$match", new Document()
                                    .append("p_size", size)
                                    .append("p_type", new Document()
                                            .append("$regex", type)
                                    )
                                    .append("r_name", "rname1")
                                    .append("ps_supplycost", mincost)
                            ),
                 new Document()
                            .append("$project", new Document()
                                    .append("s_acctbal", 1.0)
                                    .append("s_name", 1.0)
                                    .append("s_nation.n_name", 1.0)
                                    .append("_id.p_partkey", 1.0)
                                    .append("p_mfgr", 1.0)
                                    .append("s_address", 1.0)
                                    .append("s_phone", 1.0)
                                    .append("s_comment", 1.0)
                            ), 
                    new Document()
                            .append("$sort", new Document()
                                    .append("s_acctbal", -1.0)
                                    .append("n_name", 1.0)
                                    .append("s_name", 1.0)
                                    .append("_id.p_partkey", 1)
                            )
            );
         return pipeline;
    }
    private static List<? extends Bson> aggregationQuery3() {
    List<? extends Bson> pipeline = Arrays.asList(
                    new Document()
                            .append("$match", new Document()
                                    .append("c_mktsegment", "mkt1")
                                    .append("o_orderdate", new Document()
                                            .append("$lt", date1)
                                    )
                                    .append("l_shipdate", new Document()
                                            .append("$gt", date2)
                                    )
                            ), 
                    new Document()
                            .append("$group", new Document()
                                    .append("_id", new Document()
                                            .append("l_orderkey", "$_id.l_orderkey")
                                            .append("o_orderdate", "$o_orderdate")
                                            .append("o_shippriority", "$o_shippriority")
                                    )
                                    .append("revenue", new Document()
                                            .append("$sum", new Document()
                                                    .append("$multiply", Arrays.asList(
                                                            new Document()
                                                                    .append("$subtract", Arrays.asList(
                                                                            1.0,
                                                                            "$l_discount"
                                                                        )
                                                                    ),
                                                            "$l_extendedprice"
                                                        )
                                                    )
                                            )
                                    )
                            ), 
                    new Document()
                            .append("$sort", new Document()
                                    .append("revenue", -1.0)
                                    .append("o_orderdate", 1.0)
                            )
            );
    return pipeline;
        
    }
    private static List<? extends Bson> aggregationQuery4() {
     List<? extends Bson> pipeline = Arrays.asList(
                    new Document()
                            .append("$match", new Document()
                                    .append("r_name", "rname1")
                                    .append("o_orderdate", new Document()
                                            .append("$gte", date)
                                            .append("$lt", dateYear)
                                    )
                            ), 
                    new Document()
                            .append("$group", new Document()
                                    .append("_id", new Document()
                                            .append("n_name", "$n_name")
                                    )
                                    .append("revenue", new Document()
                                            .append("$sum", new Document()
                                                    .append("$multiply", Arrays.asList(
                                                            new Document()
                                                                    .append("$subtract", Arrays.asList(
                                                                            1.0,
                                                                            "$l_discount"
                                                                        )
                                                                    ),
                                                            "$l_extendedprice"
                                                        )
                                                    )
                                            )
                                    )
                            ), 
                    new Document()
                            .append("$sort", new Document()
                                    .append("revenue", -1.0)
                            )
            );
     
     return pipeline;
    } 
    
    private static void insertQuery1() {
        Document doc = new Document("_id", new Document("l_orderkey", "orderkey1")
                                            .append("l_linenumber", 1))                       
                       .append("l_linestatus", "linestatus1")
                       .append("l_returnflag", "returnflag1")
                       .append("l_quantity", 2)
                       .append("l_extendedprice", 30)
                       .append("l_discount", 0.3)
                       .append("l_tax", 0.1)
                       .append("l_shipdate", 14);
        
        Document doc2 = new Document("_id", new Document("l_orderkey", "orderkey1")
                                     .append("l_linenumber", 2))
                       .append("l_linestatus", "linestatus1")
                       .append("l_returnflag", "returnflag1")
                       .append("l_quantity", 2)
                       .append("l_extendedprice", 30)
                       .append("l_discount", 0.3)
                       .append("l_tax", 0.1)
                       .append("l_shipdate", 14);
        Document doc3 = new Document("_id", new Document("l_orderkey", "orderkey2")
                                    .append("l_linenumber", 1))
                       .append("l_linestatus", "linestatus2")
                       .append("l_returnflag", "returnflag2")
                       .append("l_quantity", 2)
                       .append("l_extendedprice", 30)
                       .append("l_discount", 0.3)
                       .append("l_tax", 0.1)
                       .append("l_shipdate", 14);
        Document doc4 = new Document("_id", new Document("l_orderkey", "orderkey3")
                                    .append("l_linenumber", 1))
                       .append("l_linestatus", "linestatus2")
                       .append("l_returnflag", "returnflag2")
                       .append("l_quantity", 2)
                       .append("l_extendedprice", 330)
                       .append("l_discount", 0.3)
                       .append("l_tax", 0.1)
                       .append("l_shipdate", 14);
        Document doc5 = new Document("_id", new Document("l_orderkey", "orderkey3")
                                    .append("l_linenumber", 4))
                       .append("l_linestatus", "linestatus2")
                       .append("l_returnflag", "returnflag3")
                       .append("l_quantity", 2)
                       .append("l_extendedprice", 32)
                       .append("l_discount", 0.3)
                       .append("l_tax", 0.1)
                       .append("l_shipdate", 14);
        Document doc6 = new Document("_id", new Document("l_orderkey", "orderkey3")
                                    .append("l_linenumber", 5))
                       .append("l_linestatus", "linestatus2")
                       .append("l_returnflag", "returnflag4")
                       .append("l_quantity", 2)
                       .append("l_extendedprice", 32)
                       .append("l_discount", 0.3)
                       .append("l_tax", 0.1)
                       .append("l_shipdate", 99);
        query1.insertOne(doc);
        query1.insertOne(doc2);
        query1.insertOne(doc3);
        query1.insertOne(doc4);
        query1.insertOne(doc5);
        query1.insertOne(doc6);
    }
    private static void insertQuery2() {
        Document doc =  new Document("_id", new Document("p_partkey", "partkey1")
                                            .append("s_suppkey", "suppkey1"))
                .append("ps_supplycost", 30)
                .append("p_mfgr", "mfgr1")
                .append("p_size", 3)
                .append("p_type", "type1")
                .append("s_name", "name1")
                .append("s_address", "address1")
                .append("s_phone", "phone1")
                .append("s_acctbal", 111)
                .append("s_comment", "comment1")
                .append("n_name", "nationname1")
                .append("r_name", region);
        Document doc2 =  new Document("_id", new Document("p_partkey", "partkey2")
                                            .append("s_suppkey", "suppkey2"))
                .append("ps_supplycost", 20)
                .append("p_mfgr", "mfgr1")
                .append("p_size", 3)
                .append("p_type", "type1")
                .append("s_name", "name1")
                .append("s_address", "address1")
                .append("s_phone", "phone1")
                .append("s_acctbal", 111)
                .append("s_comment", "comment1")
                .append("n_name", "nationname1")
                .append("r_name", region);
        Document doc3 =  new Document("_id", new Document("p_partkey", "partkey3")
                                            .append("s_suppkey", "zzzzz"))
                .append("ps_supplycost", 20)
                .append("p_mfgr", "mfgr1")
                .append("p_size", 3)
                .append("p_type", "type1")
                .append("s_name", "name1")
                .append("s_address", "address1")
                .append("s_phone", "phone1")
                .append("s_acctbal", 1112)
                .append("s_comment", "comment1")
                .append("n_name", "nationname1")
                .append("r_name", region);
        
        Document doc4 =  new Document("_id", new Document("p_partkey", "partkey4")
                                            .append("s_suppkey", "zzzzz"))
                .append("ps_supplycost", 10)
                .append("p_mfgr", "mfgr1")
                .append("p_size", 3)
                .append("p_type", "type1")
                .append("s_name", "name1")
                .append("s_address", "address1")
                .append("s_phone", "phone1")
                .append("s_acctbal", 1112)
                .append("s_comment", "comment1")
                .append("n_name", "nationname1")
                .append("r_name", "notfound");
       
        query2.insertOne(doc);
        query2.insertOne(doc2);
        query2.insertOne(doc3);
        query2.insertOne(doc4);
    }

    private static void insertQuery34() {
        Document doc =  new Document("_id", new Document("l_orderkey", "orderkey1")
                                     .append("l_linenumber", 2))
                        .append("l_returnflag", "returnflag1")
                        .append("l_linestatus", "linestatus1")
                        .append("l_quantity", 3)
                        .append("l_extendedprice", 30)
                        .append("l_discount", 0.1)
                        .append("l_tax", 0.2)
                        .append("l_shipdate", 15)
                        .append("n_name", "nationame")
                        .append("r_name", region)
                        .append("o_orderdate", 23)
                        .append("o_shippriority", 3)
                        .append("c_mktsegment", "mkt1");
        Document doc2 = new Document("_id", new Document("l_orderkey", "orderkey1")
                                     .append("l_linenumber", 1))
                        .append("l_returnflag", "returnflag1")
                        .append("l_linestatus", "linestatus1")
                        .append("l_quantity", 3)
                        .append("l_extendedprice", 30)
                        .append("l_discount", 0.1)
                        .append("l_tax", 0.2)
                        .append("l_shipdate", 15)
                        .append("n_name", "nationame")
                        .append("r_name", region)
                        .append("o_orderdate", 3)
                        .append("o_shippriority", 3)
                        .append("c_mktsegment", "mkt1");
        Document doc3 =  new Document("_id", new Document("l_orderkey", "orderkey2")
                                        .append("l_linenumber", 1))
                        .append("l_returnflag", "returnflag1")
                        .append("l_linestatus", "linestatus1")
                        .append("l_quantity", 3)
                        .append("l_extendedprice", 30)
                        .append("l_discount", 0.1)
                        .append("l_tax", 0.2)
                        .append("l_shipdate", 15)
                        .append("n_name", "nationame")
                        .append("r_name", region)
                        .append("o_orderdate", 3)
                        .append("o_shippriority", 3)
                        .append("c_mktsegment", "mkt1");
        Document doc4 = new Document("_id", new Document("l_orderkey", "orderkey3")
                                        .append("l_linenumber", 1))
                        .append("l_returnflag", "returnflag1")
                        .append("l_linestatus", "linestatus1")
                        .append("l_quantity", 3)
                        .append("l_extendedprice", 30)
                        .append("l_discount", 0.1)
                        .append("l_tax", 0.2)
                        .append("l_shipdate", 15)
                        .append("n_name", "nationame1")
                        .append("r_name", region)
                        .append("o_orderdate", 3)
                        .append("o_shippriority", 3)
                        .append("c_mktsegment", "mkt1");
        Document doc5 = new Document("_id", new Document("l_orderkey", "orderkey3")
                                    .append("l_linenumber", 2))
                        .append("l_returnflag", "returnflag1")
                        .append("l_linestatus", "linestatus1")
                        .append("l_quantity", 3)
                        .append("l_extendedprice", 30)
                        .append("l_discount", 0.1)
                        .append("l_tax", 0.2)
                        .append("l_shipdate", 15)
                        .append("n_name", "nationame2")
                        .append("r_name", region)
                        .append("o_orderdate", 33)
                        .append("o_shippriority", 3)
                        .append("c_mktsegment", "mkt1");
        Document doc6 =  new Document("_id", new Document("l_orderkey", "orderkey1")
                                    .append("l_linenumber", 3))
                        .append("l_returnflag", "returnflag1")
                        .append("l_linestatus", "linestatus1")
                        .append("l_quantity", 3)
                        .append("l_extendedprice", 30)
                        .append("l_discount", 0.1)
                        .append("l_tax", 0.2)
                        .append("l_shipdate", 15)
                        .append("n_name", "nationame2")
                        .append("r_name", region)
                        .append("o_orderdate", 33)
                        .append("o_shippriority", 3)
                        .append("c_mktsegment", "mkt1");
        Document doc7 =  new Document("_id", new Document("l_orderkey", "orderkey4")
                                         .append("l_linenumber", 3))
                        .append("l_returnflag", "returnflag1")
                        .append("l_linestatus", "linestatus1")
                        .append("l_quantity", 3)
                        .append("l_extendedprice", 30)
                        .append("l_discount", 0.1)
                        .append("l_tax", 0.2)
                        .append("l_shipdate", 15)
                        .append("n_name", "nationame3")
                        .append("r_name", region)
                        .append("o_orderdate", 30)
                        .append("o_shippriority", 3)
                        .append("c_mktsegment", "mkt1");
        Document doc8 =  new Document("_id", new Document("l_orderkey", "orderkey7")
                                        .append("l_linenumber", 3))
                        .append("l_returnflag", "returnflag1")
                        .append("l_linestatus", "linestatus1")
                        .append("l_quantity", 3)
                        .append("l_extendedprice", 70)
                        .append("l_discount", 0.1)
                        .append("l_tax", 0.2)
                        .append("l_shipdate", 15)
                        .append("n_name", "nationame3")
                        .append("r_name", region)
                        .append("o_orderdate", 30)
                        .append("o_shippriority", 3)
                        .append("c_mktsegment", "mkt1");
        query34.insertOne(doc);
        query34.insertOne(doc2);
        query34.insertOne(doc3);
        query34.insertOne(doc4);
        query34.insertOne(doc5);
        query34.insertOne(doc6);
        query34.insertOne(doc7);
        query34.insertOne(doc8);
    }

  

 

   
    
   
    
}
