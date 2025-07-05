package com.interview.timeseries;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the TimeSeriesStore implementation.
 */
public class TimeSeriesStoreTest {
    
    private TimeSeriesStore store;
    private static final File LOG_FILE = new File("data_store.log");
    
    @Before
    public void setUp() {
        // Delete any existing log so we start fresh
        if (LOG_FILE.exists()) {
            LOG_FILE.delete();
        }
        
        store = new TimeSeriesStoreImpl();
        assertTrue("Store should initialize", store.initialize());
    }
    
    @After
    public void tearDown() {
        assertTrue("Store should shutdown", store.shutdown());
        // Clean up again
        if (LOG_FILE.exists()) {
            LOG_FILE.delete();
        }
    }
    
    @Test
    public void testInsertAndQueryBasic() {
        // Insert test data
        long now = System.currentTimeMillis();
        Map<String, String> tags = new HashMap<>();
        tags.put("host", "server1");
        
        assertTrue(store.insert(now, "cpu.usage", 45.2, tags));
        
        // Query for the data
        List<DataPoint> results = store.query("cpu.usage", now, now + 1, tags);
        
        // Verify
        assertEquals(1, results.size());
        assertEquals(now, results.get(0).getTimestamp());
        assertEquals("cpu.usage", results.get(0).getMetric());
        assertEquals(45.2, results.get(0).getValue(), 0.001);
        assertEquals("server1", results.get(0).getTags().get("host"));
    }
    
    @Test
    public void testQueryTimeRange() {
        // Insert test data at different times
        long start = System.currentTimeMillis();
        Map<String, String> tags = new HashMap<>();
        tags.put("host", "server1");
        
        store.insert(start, "cpu.usage", 45.2, tags);
        store.insert(start + 1000, "cpu.usage", 48.3, tags);
        store.insert(start + 2000, "cpu.usage", 51.7, tags);
        
        // Query for a subset
        List<DataPoint> results = store.query("cpu.usage", start, start + 1500, tags);
        
        // Verify
        assertEquals(2, results.size());
    }
    
    @Test
    public void testQueryWithFilters() {
        // Insert test data with different tags
        long now = System.currentTimeMillis();
        
        Map<String, String> tags1 = new HashMap<>();
        tags1.put("host", "server1");
        tags1.put("datacenter", "us-west");
        
        Map<String, String> tags2 = new HashMap<>();
        tags2.put("host", "server2");
        tags2.put("datacenter", "us-west");
        
        store.insert(now, "cpu.usage", 45.2, tags1);
        store.insert(now, "cpu.usage", 42.1, tags2);
        
        // Query with filter on datacenter
        Map<String, String> filter = new HashMap<>();
        filter.put("datacenter", "us-west");
        
        List<DataPoint> results = store.query("cpu.usage", now, now + 1, filter);
        
        // Verify
        System.out.println("Results size: " + results.size());
        assertEquals(2, results.size());
        
        // Query with filter on host
        filter.clear();
        filter.put("host", "server1");
        
        results = store.query("cpu.usage", now, now + 1, filter);
        
        // Verify
        assertEquals(1, results.size());
        assertEquals("server1", results.get(0).getTags().get("host"));
    }

    @Test
    public void testMultipleFilters() {
        long ts = System.currentTimeMillis();
        // Insert three points with various tags
        store.insert(ts,   "m", 1.0, Map.of("a","x","b","y"));
        store.insert(ts+1, "m", 2.0, Map.of("a","x","b","z"));
        store.insert(ts+2, "m", 3.0, Map.of("a","q","b","y"));

        // filter a=x, b=y, should result only the first point
        Map<String,String> f1 = new HashMap<>();
        f1.put("a","x");
        f1.put("b","y");
        List<DataPoint> r1 = store.query("m", ts, ts+3, f1);
        assertEquals(1, r1.size());
        assertEquals(1.0, r1.get(0).getValue(), 0.0);

        // Filter a=x only, should return first two points
        List<DataPoint> r2 = store.query("m", ts, ts+3, Map.of("a","x"));
        assertEquals(2, r2.size());
        Set<Double> vals = new HashSet<>();
        for (DataPoint dp : r2) vals.add(dp.getValue());
        assertTrue(vals.containsAll(List.of(1.0,2.0)));
    }
    
    @Test
    public void testTimeBoundsInclusiveExclusive() {
        long t0 = 1_000L;
        // insert at t0 and t0+1000
        store.insert(t0, "m", 1.0, null);
        store.insert(t0 + 1000, "m", 2.0, null);

        // should only query 1 result as end time is exclusive
        List<DataPoint> a = store.query("m", t0, t0 + 1000, null);
        assertEquals(1, a.size());
        assertEquals(1.0, a.get(0).getValue(), 1e-9);

        // should query both
        List<DataPoint> b = store.query("m", t0, t0 + 1001, null);
        assertEquals(2, b.size());
    }

    @Test
    public void testPersistenceAcrossRestart() {
        long now = System.currentTimeMillis();
        Map<String,String> tags = Map.of("k","v");
        store.insert(now, "persist", 3.3, tags);
        store.shutdown();

        // new instance should read the log
        store = new TimeSeriesStoreImpl();
        assertTrue(store.initialize());
        List<DataPoint> res = store.query("persist", now, now + 1, tags);
        assertEquals(1, res.size());
        DataPoint dp = res.get(0);
        assertEquals(now, dp.getTimestamp());
        assertEquals(3.3, dp.getValue(), 1e-9);
        assertEquals("v", dp.getTags().get("k"));
    }

    @Test
    public void testHighCardinalityTagFilter() {
        long base = 1000L;
        // create 10 Million entries with different repeating tags
        IntStream.range(0, 100000).forEach(i ->
            store.insert(base + i, "high", i, Map.of("uid", "user" + (i % 10)))
        );
        // query for a specific tag value
        List<DataPoint> res = store.query("high", base, base + 100000, Map.of("uid", "user5"));
        assertEquals(10000, res.size());
        res.forEach(dp -> assertEquals("user5", dp.getTags().get("uid")));
    }

    
}
