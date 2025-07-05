package com.interview.timeseries;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Optimized TimeSeriesStore implementation.
 * 
 */
public class TimeSeriesStoreImpl implements TimeSeriesStore {
    private static final String LOG_FILE = "data_store.log";
    private static long retentionTime = 24L * 60 * 60 * 1000; // 24h

    // flat store: metric -> time-sorted list of DataPoint
    private final ConcurrentMap<String, List<DataPoint>> flatStore = new ConcurrentHashMap<>();
    // bitmaps: metric -> tagKey -> tagValue -> BitSet of indices in flatStore list
    private final ConcurrentMap<
        String,
        ConcurrentMap<String, ConcurrentMap<String, BitSet>>> tagBitmaps = new ConcurrentHashMap<>();

    // read-write lock for thread safety
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    // writer for persistence
    private BufferedWriter writer;

    // configure retention time in milliseconds
    public static void setRetentionTime(long millis) {
        retentionTime = millis;
    }

    @Override
    public boolean initialize() {
        rwLock.writeLock().lock();
        try {
            Path path = Paths.get(LOG_FILE);
            if (Files.exists(path)) {
                System.out.println("Recovering from existing log file: " + LOG_FILE);
                try (BufferedReader reader = Files.newBufferedReader(path)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        DataPoint dp = parseLine(line);
                        indexInMemory(dp);
                    }
                }
                setRetentionTime(retentionTime);
                evictOldData();
            }
            writer = Files.newBufferedWriter(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
            );
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public boolean insert(long timestamp, String metric, double value, Map<String, String> tags) {
        DataPoint dp = new DataPoint(timestamp, metric, value, tags);
        rwLock.writeLock().lock();
        try {
            indexInMemory(dp);
            writer.write(dp.toString()); writer.newLine(); writer.flush();
            return true;
        } catch (IOException e) {
            e.printStackTrace(); return false;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public List<DataPoint> query(String metric, long timeStart, long timeEnd, Map<String, String> filters) {
        rwLock.readLock().lock();
        try {
            List<DataPoint> flat = flatStore.get(metric);
            if (flat == null || flat.isEmpty()) {
                return Collections.emptyList();
            }
            // no filters: binary search on flat list
            if (filters == null || filters.isEmpty()) {
                int lo = findFirstIndex(flat, timeStart);
                int hi = findFirstIndex(flat, timeEnd);
                return flat.subList(lo, hi);
            }
            // build combined BitSet
            ConcurrentMap<String, ConcurrentMap<String, BitSet>> metricMap = tagBitmaps.get(metric);
            if (metricMap == null) return Collections.emptyList();
            BitSet combined = null;
            // purpose of combined bitset : 
            // for each filter, find the bitset for the tag value and combine them
            // using AND operation to find indices that match all filters
            for (Map.Entry<String, String> f : filters.entrySet()) {
                ConcurrentMap<String, BitSet> byVal = metricMap.get(f.getKey());
                if (byVal == null) return Collections.emptyList();
                BitSet bs = byVal.get(f.getValue());
                if (bs == null) return Collections.emptyList();
                if (combined == null) combined = (BitSet) bs.clone();
                else combined.and(bs);
                if (combined.isEmpty()) return Collections.emptyList();
            }
            // time boundaries
            int lo = findFirstIndex(flat, timeStart);
            int hi = findFirstIndex(flat, timeEnd);
            List<DataPoint> result = new ArrayList<>();
            for (int idx = combined.nextSetBit(lo); idx >= 0 && idx < hi; idx = combined.nextSetBit(idx + 1)) {
                result.add(flat.get(idx));
            }
            return result;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public boolean shutdown() {
        rwLock.writeLock().lock();
        try {
            if (writer != null) writer.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace(); return false;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Evict old data from flatStore and clear tagBitmaps
     * 
     */
    private void evictOldData() {
        long cutoff = System.currentTimeMillis() - retentionTime;
        // main flat store
        for (Map.Entry<String, List<DataPoint>> e : flatStore.entrySet()) {
            List<DataPoint> list = e.getValue();
            int idx = findFirstIndex(list, cutoff);
            if (idx > 0) list.subList(0, idx).clear();
        }

        for (String metric : tagBitmaps.keySet()) {
            List<DataPoint> list = flatStore.get(metric);
            ConcurrentMap<String, ConcurrentMap<String, BitSet>> mMap = tagBitmaps.get(metric);
            mMap.values().forEach(byVal -> byVal.values().forEach(bs -> bs.clear()));
            for (int i = 0; i < list.size(); i++) {
                DataPoint dp = list.get(i);
                ConcurrentMap<String, ConcurrentMap<String, BitSet>> mm = tagBitmaps.get(metric);
                for (Map.Entry<String, String> tag : dp.getTags().entrySet()) {
                    mm.get(tag.getKey()).get(tag.getValue()).set(i);
                }
            }
        }
    }

    /**
     * Index dp into flatStore and tagBitmaps under write lock
     */
    private void indexInMemory(DataPoint dp) {
        List<DataPoint> flat = flatStore.computeIfAbsent(dp.getMetric(), k -> new ArrayList<>());
        flat.add(dp);
        int idx = flat.size() - 1;
        ConcurrentMap<String, ConcurrentMap<String, BitSet>> metricMap =
            tagBitmaps.computeIfAbsent(dp.getMetric(), k -> new ConcurrentHashMap<>());
        for (Map.Entry<String, String> tag : dp.getTags().entrySet()) {
            ConcurrentMap<String, BitSet> byVal =
                metricMap.computeIfAbsent(tag.getKey(), k -> new ConcurrentHashMap<>());
            BitSet bs = byVal.computeIfAbsent(tag.getValue(), v -> new BitSet());
            bs.set(idx);
        }
    }

    /**
     * Find first index with timestamp >= time using binary search
     * @param list The list to search
     * @param time The timestamp to find
     * @return The index of the first element with timestamp >= time
     */
    private int findFirstIndex(List<DataPoint> list, long time) {
        int lo = 0, hi = list.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (list.get(mid).getTimestamp() < time) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /**
     * Parse a line from the log file into a DataPoint object.
     * 
     * @param line The line to parse
     * @return A DataPoint object
     */
    private DataPoint parseLine(String line) {
        String body = line.substring(line.indexOf('{')+1, line.lastIndexOf('}'));
        String[] parts = body.split(", (?=(?:[^\\{]*\\{[^\\}]*\\})*[^\\}]*$)",4);
        long ts = Long.parseLong(parts[0].split("=",2)[1]);
        String metric = parts[1].split("=",2)[1].replaceAll("^'|'$", "");
        double val = Double.parseDouble(parts[2].split("=",2)[1]);
        String tagsBody = parts.length==4? parts[3].substring(parts[3].indexOf('{')+1, parts[3].lastIndexOf('}')):"";
        Map<String,String> tags = new HashMap<>();
        if(!tagsBody.isEmpty()){
            for(String kv: tagsBody.split(", ")){
                String[] p=kv.split("=",2); tags.put(p[0],p[1]);
            }
        }
        return new DataPoint(ts, metric, val, tags);
    }

    /**
     * Get the flat store for testing purposes.
     * This is currently used by BenchmarkStore.java
     * @return The flat store map
     */
    public Map<String, List<DataPoint>> getStore() {
        return flatStore;
    }
}
