package flink.country.count;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.batch.connectors.cassandra.CassandraOutputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;

/**
 * created by Matko Sorić
 *
 * data batch transport from HBase to Cassandra
 *
 *
 */
@SuppressWarnings("serial")
public class CitiesETL {

    public static void main(String[] args) throws Exception {

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

        DataSet<Tuple2<String, Long>> events = citiesPhoenix.flatMap(new FlatMapFunction<Row, Tuple2<String, Long>>() {

            @Override
            public void flatMap(Row row, Collector<Tuple2<String, Long>> collector) throws Exception {

                String country = (String)row.getField(3);
                Long population = (Long)row.getField(7);

                if (country == null) country = "";
                if (population == null) population = 0L;

                collector.collect(new Tuple2<String, Long>(country, population));
            }

        })
                .groupBy(0)
                .sum(1);

        CassandraOutputFormat sink = new CassandraOutputFormat("INSERT INTO counting.cities(country, count) values (?, ?);", new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                builder.addContactPoint("127.0.0.1");
                return builder.build();
            }
        });

        events.output(sink);

        environment.execute("Batch ETL of World Bank Cities data from HBase/Phoenix to Cassandra");

    }
}