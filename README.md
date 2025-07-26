# Gleam: Fast-light in memory DataFrame in Go
Gleam is a Go Dataframe library based on Apache Arrow.  
This library focuses on the speed to operation with Arrow memory design 
and goroutine multithreading.



## Load map for development
### v0.1.0
#### Core Data Structure
 - [ ] `DataFrame` and `Series` structures
#### I/O
 - [ ] `ReadCSV` to load data from CSV files, including support for schema inference
 - [ ] `NewDataFrame` from Go slice
 - [ ] `String` for pretty-printing a DataFrame
 - [ ] `WriteCSV` to write data to CSV file
#### Operations
 - [ ] `Select` to select column(s)
 - [ ] `Where` to filter row
 - [ ] `Sum`, `Mean`, `Min`, `Max`, `Count` basic column-wise aggregations
 - [ ] `Sort` to sort the DataFrame by one or more columns
 - [ ] `Drop` to remove column(s)
 - [ ] Cast data types for `Series`
### Null Detection
 - [ ] `IsNull`, `IsNotNull` provides ability to detect the missing values
### DataType
 - [ ] float type: `Float32`, `Float64`
 - [ ] integer type: `Int8`, `Int16`, `Int32`, `Int64`, `Int128`
 - [ ] unsign-integer type: `Uint8`, `Uint16`, `Uint32`, `Uint64`
 - [ ] `String`
 - [ ] `Boolean`
 - [ ] `Null`

### v0.2.0
#### Grouping
 - [ ] `GroupBy` to group the data by one or more columns
 - [ ] `GroupedDataFrame` which specialized for aggregations(`agg`)
   - Support and optimize applying aggregation functions (concurrently apply)
#### Join
 - [ ] `InnerJoin`, `LeftJoin`, `RightJoin` to combine multiple DataFrames 
#### I/O
 - [ ] `ReadParquet` to load data from a parquet file
 - [ ] `WriteParquet` to write data to a parquet file
#### Null Filling
 - [ ] `FillNull` supports filling missing values
#### DataType
 - [ ] `Decimal`
 - [ ] `Datetime`
 - [ ] `Duration`
 - [ ] `Time`

### v0.3.0
#### I/O
 - Expanding the source data format
   - [ ] `json`
   - [ ] `excel`
#### Join
 - [ ] `OuterJoin`, `CrossJoin` extends the combining multiple DataFrames
#### DataType
 - [ ] `Binary`
 - [ ] `Array`

### v0.4.0
#### Performance
 - [ ] Introduce Lazy evaluation for operations
 - [ ] Streaming load
 - [ ] Thoroughly using **SIMD** for GroupBy

### v0.5.0 or later
#### I/O
 - [ ] `database`
   - [ ] Postgres
   - [ ] MySQL
   - [ ] SQLServer
   - [ ] Snowflake
 - [ ] `map` (Go) 
#### DataType
 - [ ] Expand DataType for a corner case
