package flinkcountrycount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.util.Collector;

/**
 *  working example of data stream from csv to Cassandra
 *
 *
 *
 */
@SuppressWarnings("serial")
public class SocketWindowCountryCount {

    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> citiesCsv = env.readTextFile("src/main/resources/world_bank_cities.csv");

        DataStream<Tuple2<String, Long>> events = citiesCsv
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                        String[] preparedLine = value.toLowerCase().split(",");

                        if (preparedLine.length > 7) {
                            if (!preparedLine[7].contains("census")) {
                                String country = preparedLine[3].substring(1, preparedLine[3].length() - 1);

                                Long population = Long.parseLong(preparedLine[7]);

                                out.collect(new Tuple2<String, Long>(country, population));
                            }
                        }
                    }
                })
                .keyBy(0)
//                .countWindow(50)
//                .timeWindow(Time.milliseconds(50))
                .sum(1);

        CassandraSink.addSink(events)
                .setQuery("INSERT INTO counting.cities(country, count) values (?, ?);")
                .setHost("127.0.0.1")
                .build();

        env.execute("World Bank Cities Csv to standard output");

    }
}
