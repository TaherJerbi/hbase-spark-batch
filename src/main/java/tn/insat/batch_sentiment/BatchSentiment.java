package tn.insat.batch_sentiment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class BatchSentiment {

    public void countSentiments() {
        Configuration config = HBaseConfiguration.create();
        SparkConf sparkConf = new SparkConf().setAppName("SparkHBaseTest").setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        config.set(TableInputFormat.INPUT_TABLE,"comments_raw");

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        // perform sentiment analysis on each comment body and count the number of comments with each sentiment
        // IN: comment_id, comment_parent_id, comment_body, subreddit, timestamp
        // OUT: comment_id, comment_parent_id, comment_body, subreddit, timestamp, sentiment

        SentimentAnalyzer analyzer = SentimentAnalyzer.getInstance();

        JavaPairRDD<String, Integer> sentimentCounts = hBaseRDD.mapToPair(
                tuple -> {
                    String text = new String(tuple._2.getValue("cf".getBytes(), "comment_body".getBytes()));
                    System.out.println("Text: "+text);
                    String sentiment = analyzer.getMajoritySentiment(text);
                    return new Tuple2<String, Integer>(sentiment, 1);
                }
        ).reduceByKey(
                (x, y) -> x + y
        );

        // save the results to HBase
        HbaseRepository hbaseRepository = HbaseRepository.getInstance();
        hbaseRepository.insertResult(sentimentCounts.collect());
    }

    public static void main(String[] args){
        BatchSentiment admin = new BatchSentiment();
        admin.countSentiments();
    }

}