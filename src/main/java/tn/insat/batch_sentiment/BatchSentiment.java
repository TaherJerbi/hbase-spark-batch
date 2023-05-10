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
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class BatchSentiment {

    public Producer makeProducer() {
        // Creer une instance de proprietes pour acceder aux configurations du producteur
        Properties props = new Properties();

        // Assigner l'identifiant du serveur kafka
        props.put("bootstrap.servers", "kafka:9092");

        // Definir un acquittement pour les requetes du producteur
        props.put("acks", "all");

        // Si la requete echoue, le producteur peut reessayer automatiquemt
        props.put("retries", 0);

        // Specifier la taille du buffer size dans la config
        props.put("batch.size", 16384);

        // buffer.memory controle le montant total de memoire disponible au producteur pour le buffering
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");


        return new KafkaProducer<String, String>(props);
    }
    public void countSentiments() {
        Configuration config = HBaseConfiguration.create();
        SparkConf sparkConf = new SparkConf().setAppName("SparkHBaseTest").setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        config.set(TableInputFormat.INPUT_TABLE,"comments_sentiments");

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        // perform sentiment analysis on each comment body and count the number of comments with each sentiment
        // IN: comment_id, comment_parent_id, comment_body, subreddit, timestamp
        // OUT: comment_id, comment_parent_id, comment_body, subreddit, timestamp, sentiment

        JavaPairRDD<String, Integer> sentimentCounts = hBaseRDD.mapToPair(
                tuple -> {
                    String sentiment = new String(tuple._2.getValue("cf".getBytes(), "sentiment".getBytes()));
                    System.out.println("Sentiment: " + sentiment);
                    return new Tuple2<String, Integer>(sentiment, 1);
                }
        ).reduceByKey(
                (x, y) -> x + y
        );

        // save the results to HBase
        HbaseRepository hbaseRepository = HbaseRepository.getInstance();
        hbaseRepository.insertResult(sentimentCounts.collect());

        // send the results to Kafka
        Producer<String, String> producer = makeProducer();
        String message = sentimentCounts.collect().toString();
        producer.send(new ProducerRecord<String, String>("batch_result", message));
    }

    public static void main(String[] args){
        BatchSentiment admin = new BatchSentiment();
        admin.countSentiments();
    }

}