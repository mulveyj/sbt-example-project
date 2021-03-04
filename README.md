# New Day Demonstration Task

The purpose of this application is to demonstrate a simple Spark job that reads in 
some data, performs basic aggregations and writes to Parquet.

The brief provided was that the app should:
1.	Read in movies.csv and ratings.csv to spark dataframes called movies and ratings
      respectively
2.	Create a new dataframe called movieRatings, containing the movies data and 3 
      new columns which contain the max, min and average rating for that movie from the ratings data.
3.	BONUS: Create a new dataframe which contains each user’s (userId in the 
      ratings data) top 3 movies based on their rating. __NOT COMPLETED__
4.	Write out the original and new dataframes to parquet format.

### Prerequisites
The app is written in Spark 3.1.0 with Scala 2.12.13. It is intended to be run in
a POSIX-style environment: Linux, MacOS, WSL.

### To run locally
Copies of the raw data files `movies.dat` and `ratings.dat` are expected to be 
available in the local filesystem. In the shell, execute this, replacing the paths with 
the correct destinations:
```bash
sbt run path/to/movies.dat path/to/ratings.dat path/to/output/dir
```
The output parquet files will be located in the directories:
```
path/to/output/dir/movies
path/to/output/dir/ratings
path/to/output/dir/ratingsreport
```
### To compile a Jar
Run:
```bash
sbt assembly
```
***NOT WORKING***

### To run in a Spark cluster
Use the following command:
```bash
/path/to/spark-home/bin/spark-submit \
  --class com.newday.example.Main \
  --name workdaydemo \
  --master <master url> \
  ./target/scala-2.11/<jar name> \
  <input file> <output file>
  ```


