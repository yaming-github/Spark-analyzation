# CS236 Class Project

## Student information

* Full name:
```
Yaming Zhang
``` 
* E-mail:
```
yzhan737@ucr.edu
```
* UCR NetID:
```
yzhan737
```
* Student ID:
```
X674002
```

## Prerequisites
* Java 1.8
* Maven 3.6
* Scala 2.12
* Spark 2.4.5
* Download the real datasets [here](https://drive.google.com/open?id=1TxS1XeKwykrGV8qZsvm741Dz4p8rT9WT)

## Details
* Run the program by
```shell script
./run.sh ${locations file directory} ${recordings file ditrctory} ${output directory}
```

* Approximate running time
```
225.52 seconds
```

* Full logic for the whole project
```
In the very beginning of the project, we read the first 2 arguments as our inputs and the last argument as our output directory.
Then we set the spark configuration and create the SparkSession for later processing.
```

```
load the .csv file.
For the .csv file, we only need the USAF and the STATE. So, after we filter the stations in the US, we only need to reserve
these two attributes using #select function for the following calculation. Discarding other attributes here could help improve
the following calculation performance.

The data after processing looks like this.
```
| USAF | STATE |
| :----: | :---: |
| 720753 | OR |
| 720754 | MN |
| ... | ... |

```
load the 4 .txt files.
Note that since the .txt file has no schema and may have multiple headers, we have to process these "raw" data in a brute force way. 
For headers, we have to filter the lines that starts with "STN---", which are the headers.
For precipitation, we have to locate the position of the attribute PRCP and access the data using the #substring function.
For example, we can use #s.substring(0, 6) as USAF and #s.substring(119, 123) as PRCP.

Specifically, for the calculation of the precipitation, we first skip all the data of value 99.99 and then we access the
letter after the PRCP to calculate the precipitation. I use the simple solution to multiply. If the record is of 12 hours,
I will muliply it by 2 to extrapolate the precipitation of the whole day.

From line 55 to line 173, I calculate the precipitation of each station in each month of each year using the same algorithms.

For the data of 4 years, I union them together, group by station and month and calculate the average precipitation of each station.

The data after processing looks like this.
```
| USAF | month | PRCP |
| :----: | :---: | :---: |
| 010010 | 01 | 6.34 |
| 010010 | 02 | 8.93 |
| ... | ... | ... |
| 722149 | 09 | 10.34 |
| 722149 | 10 | 16.93 |
| ... | ... | ... |

```
Till now, we have process all the data from .csv file and .txt files.
We have data of USAF and STATE indicating which state do the stations belong to.
We have USAF, month and PRCP indicating the precipitation data of each station.
Now, we can join the two tables to access the precipitation of each state.
```

```
As I mentioned, we join the 2 tables on USAF and group by the STATE and month.
Then we add all the precipitation data in the state of different stations to show the precipitation of the state.

Now, we have a table 
```
| STATE | month | PRCP |
| :----: | :---: | :---: |
| SC | 01 | 90.20 |
| SC | 05 | 103.33 |
| ... | ... | ... |
| OR | 05 | 67.89 |
| HI | 11 | 63.29 |
| ... | ... | ... |

```
Since we have the precipitation of each state, we can now find the maximum and minimum precipitation of each state.
So, we have one table to store the maximum PRCP and one table to store minimum PRCP.
Then we need to join the two tables to calculate the delta of the max and min.

Here, we need to write a UDF to calculate the difference between 2 attributes.
val diffUDF = udf((max: Double, min: Double) => (max - min).formatted("%.2f").toDouble)
```

```
We join the 2 tables with max PRCP and min PRCP together and add an attribute using my UDF as the delta between max and min.
Then we order all the data by Diff in ascending order and write the final output to the output directory.

The final result looks like this
```
|STATE|Max|Min|Diff|
| :----: | :---: | :---: | :---:|
|DC|0.00|0.00|0.0|
|VI|8.28|1.27|7.01|
|NV|20.57|6.87|13.7|
|RI|35.85|19.66|16.19|
|PR|20.48|0.84|19.64|
|...|...|...|...|