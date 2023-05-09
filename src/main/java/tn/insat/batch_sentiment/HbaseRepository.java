package tn.insat.batch_sentiment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;

import java.util.List;

public class HbaseRepository {
    // singleton instance
    private static HbaseRepository instance = null;
    private static Configuration config = null;
    private static Connection connection = null;
    private static Admin admin = null;
    private static Table mr_result_table = null;
    private static final String MR_RESULT_TABLE_NAME = "mr_job_sentiment_analysis";

    // private constructor
    private HbaseRepository() {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
            mr_result_table = connection.getTable(TableName.valueOf(MR_RESULT_TABLE_NAME));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // public static method to get the singleton instance
    public static HbaseRepository getInstance() {
        if (instance == null) {
            instance = new HbaseRepository();
        }
        return instance;
    }

    // insert a raw comment into HBase
    // arg: comment (comment_id, parent_id, comment_body, subreddit, timestamp)
    public void insertResult(List<Tuple2<String,Integer>> sentimentCounts) {
        try {
            System.out.println("Inserting result into HBase...");
            // create the row key
            String rowKey = String.valueOf(System.currentTimeMillis());
            Put put = new Put(Bytes.toBytes(rowKey));

            sentimentCounts.forEach(
                    tuple -> {
                        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(tuple._1), Bytes.toBytes(tuple._2.toString()));
                    }
            );

            mr_result_table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
