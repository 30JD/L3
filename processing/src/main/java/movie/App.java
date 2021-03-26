package movie;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import java.util.List;
import java.util.Properties;
import java.util.Timer;

/**
 * Hello world!
 *
 */
public class App 
{
    private static long startTime;
    private static class MySimpleStringSchema extends SimpleStringSchema {
        private static final long serialVersionUID = 1L;

        @Override
        public String deserialize(byte[] message) {

            return super.deserialize(message);
        }

        @Override
        public boolean isEndOfStream(String nextElement) {
            if (System.currentTimeMillis() - startTime >= 60000) {
                return true;
            }
            return super.isEndOfStream(nextElement);
        }
    }
    public static void main( String[] args ) throws Exception
    {
        String host = "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", host);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>("movielog3", new MySimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);
        startTime = System.currentTimeMillis();
        stream.map(new Splitter())
                .filter(new nullFilter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .sum(1)
                .map(new Rearranger())
                .writeAsText("data.txt", WriteMode.OVERWRITE)
                .setParallelism(1);
        env.execute();
    }

    public static class nullFilter implements FilterFunction<Tuple2<String, Integer>> {
        @Override
        public boolean filter(Tuple2<String, Integer> tuple) throws Exception {
            return tuple.f0 != null;
        }
    }

    public static class Splitter implements MapFunction<String, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
            String[] strList = value.split(",");
            if (strList.length != 3) {
                return new Tuple2<>();
            } else {
                if (strList[2].substring(0, 9).equals("GET /data")) {
                    String userId = strList[1];
                    String[] dataStrs = strList[2].split("/");
                    String movieId = dataStrs[dataStrs.length - 2];
                    String userMovie = userId + "/" + movieId;
                    return new Tuple2<>(userMovie, 1);
                } else {
                    return new Tuple2<>();
                }
            }

        }
    }

    public static class Rearranger implements MapFunction<Tuple2<String, Integer>, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> map(Tuple2<String, Integer> value) throws Exception{
            String[] userMovieStrs = value.f0.split("/");
            String userId = userMovieStrs[0];
            String movieId = userMovieStrs[1];
            return new Tuple2<>(userId, movieId + ":" + String.valueOf(value.f1));
        }
    }

}
