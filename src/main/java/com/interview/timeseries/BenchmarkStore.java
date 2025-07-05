package com.interview.timeseries;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * reads time_series_data.csv which is generated from python script
 * also benchmarks the TimeSeriesStoreImpl as per given requirements
 * 
 */
public class BenchmarkStore {
    private static final String CSV = "time_series_data.csv";
    private static final int QUERY_COUNT = 1_000; // number of queries to issue

    public static void main(String[] args) throws Exception {
        Path path = Paths.get(CSV);
        if (!Files.exists(path)) {
            System.err.println("CSV file not found: " + CSV);
            return;
        }

        // initialize store
        TimeSeriesStoreImpl store = new TimeSeriesStoreImpl();
        store.initialize();

        BufferedReader reader = Files.newBufferedReader(path);
        String headerLine = reader.readLine();
        String[] headers = headerLine.split(",");
        List<String> tagKeys = Arrays.asList(headers).subList(3, headers.length);

        // load & insert all rows, measuring throughput
        long count = 0;
        long startNs = System.nanoTime();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            long ts = Long.parseLong(parts[0]) * 1000;
            String metric = parts[1];
            double value = Double.parseDouble(parts[2]);

            // build tags
            Map<String, String> tags = new HashMap<>();
            int flag = 0; // to track if any tag was added`
            for (int i = 0; i < tagKeys.size() && 3 + i < parts.length; i++) {
                String key = tagKeys.get(i);
                String val = parts[3 + i];
                if (val != null && !val.isEmpty()) {
                    tags.put(key, val);
                    flag = 1;
                }
                // limit to 1, as there can be too many tags
                if (flag == 1) {
                    break;
                }
            }
            store.insert(ts, metric, value, tags);
            count++;
        }
        long durationNs = System.nanoTime() - startNs;
        double secs = durationNs / 1e9;
        System.out.println(
            String.format("Inserted %,d rows in %.2f s, %.2f writes/sec",
                count, secs, count/secs
            )
        );

        // count distinct metrics
        int distinctMetrics = ((TimeSeriesStoreImpl) store).getStore().keySet().size();
        System.out.println(
            "Distinct metrics: " + distinctMetrics
        );

        // run queries without filters
        int qcount = QUERY_COUNT;
        long qStartNs = System.nanoTime();
        for (int i = 0; i < qcount; i++) {
            long now   = System.currentTimeMillis();
            long from  = now - TimeUnit.HOURS.toMillis(24);
            store.query("temperature", from, now, null);
        }
        long qDurationNs = System.nanoTime() - qStartNs;
        double qSecs = qDurationNs / 1e9;
        System.out.println(
            String.format("Ran %,d normal queries in %.2f s, %.2f qps",
                qcount, qSecs, qcount/qSecs
            )
        );

        // run queries with hardcoded filters
        qcount = QUERY_COUNT;
        Map<String, String> filter = Map.of(
            "datacenter", "eu-central"
        );
        qStartNs = System.nanoTime();
        for (int i = 0; i < qcount; i++) {
            long now   = System.currentTimeMillis();
            long from  = now - TimeUnit.HOURS.toMillis(24);
            store.query("temperature", from, now, filter);
        }
        qDurationNs = System.nanoTime() - qStartNs;
        qSecs = qDurationNs / 1e9;
        System.out.println(
            String.format("Ran %,d filtered queries in %.2f s, %.2f qps",
                qcount, qSecs, qcount/qSecs
            )
        );

        store.shutdown();
    }
}
