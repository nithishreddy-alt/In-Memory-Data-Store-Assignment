# Time Series Store Interview Project

This project includes implementation of TimeSeriesStore.java to support efficient read and write using thread safety locks with write persistance to survive restarts

## Git Clone and build the project in your local

Please note that, I have used Java 19 as gradle version 7.6 was provided for the development

```bash
# On Linux/Mac
./gradlew build

# On Windows
gradlew.bat build

# Running tests
./gradlew test
```

## Data Structures Used
### 1. Flat Store
  - Data structure representation : `ConcurrentMap<String, List<DataPoint>>`, mapping each metric to a time-sorted `ArrayList<DataPoint>`
  - Data storage representation : `flatStore.get("cpu.usage"): [DataPoint{}, DataPoint{},]`
### 2. Tag Bitmaps
  - Data structure representation : `ConcurrentMap<String, Map<String, BitSet>>`, where each Bitset maps to tagKey-tagValue to indices in Flat Store
  - Data storage representation : `tagBitmaps.get("cpu.usage").get("host").get("server1"): [0, 3, 7 ..]`
   

## Time Complexities for corresponding Operations
1. Insert : O(T) 
  - reason : O(1) -flat Store * O(T) - TagBitMaps, T is no. of tags
2. Query without filters : O(log N + R)
  - reason : Binary Search on sorted list takes O(log N) and R is to retrieve sub list
3. Query with filters : O(F*B + logN + R)
  - reason : O(F·B) for cloning F bitsets of length B, O(log N) for the binary search on the flat list, O(R) to scan and collect the R matching datapoints.


## Test Results on ~ 5 Million DataPoints through BenchmarkStore.java

- Inserted 5,040,875 rows in 37.68 s, 133793.03 writes/sec
- Ran 1,000 normal queries in 0.00 s, 577634.01 queries/sec
- Ran 1,000 filtered queries in 0.84 s, 1185.05 queries/sec

Instructions To Verify above results: 
1. Run python script `generate_sample_data.py` which creates time_series_data.csv
2. Run BenchmarkStore.java 
