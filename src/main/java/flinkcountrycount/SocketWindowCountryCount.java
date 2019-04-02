package flinkcountrycount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.batch.connectors.cassandra.CassandraOutputFormat;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;



/**
 *
 * working example of data stream from csv to Cassandra
 *
 *
 */
@SuppressWarnings("serial")
public class SocketWindowCountryCount {

    public static void main(String[] args) throws Exception {


//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<String> citiesCsv = env.readTextFile("src/main/resources/world_bank_cities.csv");

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();


            TypeInformation[] fieldTypes = new TypeInformation[] {
                STRING_TYPE_INFO,
                STRING_TYPE_INFO,
                STRING_TYPE_INFO,
                STRING_TYPE_INFO,
                DOUBLE_TYPE_INFO,
                DOUBLE_TYPE_INFO,
                LONG_TYPE_INFO,
                LONG_TYPE_INFO
                };

    RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        DataSet<Row> citiesPhoenix
                = environment.createInput(JDBCInputFormat.buildJDBCInputFormat()
                        .setDrivername("org.apache.phoenix.jdbc.PhoenixDriver")
                        .setDBUrl("jdbc:phoenix:localhost")
                        .setQuery("select * from cities")
                        .setRowTypeInfo(rowTypeInfo)
                        .finish());

        citiesPhoenix.printToErr();





//        DataSet<Tuple2<String, Long>> events = citiesPhoenix
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
//                    @Override
//                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
//                        String[] preparedLine = value.toLowerCase().split(",");
//
//                        if (preparedLine.length > 7) {
//                            if (!preparedLine[7].contains("census")) {
//                                String country = preparedLine[3].substring(1, preparedLine[3].length() - 1);
//
//                                Long population = Long.parseLong(preparedLine[7]);
//
//                                out.collect(new Tuple2<String, Long>(country, population));
//
//
//                            }
//                        }
//                    }
//                })
//                .keyBy(3)
////                .countWindow(50)
////                .timeWindow(Time.millisecon ds(50))
//                .sum(7);
////
////        events.printToErr();
//
//
//        CassandraSink.addSink(events)
//                .setQuery("INSERT INTO counting.cities(country, count) values (?, ?);")
//                .setHost("127.0.0.1")
//                .build();

        environment.execute("World Bank Cities Csv to standard output");

    }
}





//    TypeInformation[] fieldTypes = new TypeInformation[] {
//            BasicTypeInfo.INT_TYPE_INFO,
//            BasicTypeInfo.STRING_TYPE_INFO,
//            BasicTypeInfo.STRING_TYPE_INFO,
//            BasicTypeInfo.DOUBLE_TYPE_INFO,
//            BasicTypeInfo.INT_TYPE_INFO
//    };
//
//    RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
//
//    JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
//            .setDrivername("org.apache.phoenix.jdbc.PhoenixDriver")
//            .setDBUrl("dbc:phoenix:localhost")
//            .setQuery("select * from cities")
//            .setRowTypeInfo(rowTypeInfo)
//            .finish();
