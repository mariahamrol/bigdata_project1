package org.example;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Agregator łączący: sumuje składowe dla klucza.
 * Wejście i wyjście w formacie mappera: "orders_count,total_price_sum,cancelled_count",
 * dzięki czemu combiner może być stosowany wielokrotnie.
 */
public class OrdersCombiner
            extends Reducer<Text, Text, Text, Text> {

        private final Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            long ordersCount = 0;
            double totalSum = 0;
            long cancelledCount = 0;

            for (Text value : values) {
                String[] fields = value.toString().split(",");

                ordersCount += Long.parseLong(fields[0]);
                totalSum += Double.parseDouble(fields[1]);
                cancelledCount += Long.parseLong(fields[2]);
            }

            result.set(
                    ordersCount + "," + totalSum + "," + cancelledCount
            );

            context.write(key, result);
        }
    }
