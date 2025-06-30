# Spark_Application

## Overview

This repository contains a **Spark application** designed to process **transaction data**, **clean and transform** it, and join it with **subscriber data** from a database. The resulting dataset is saved in **Parquet** file format.

## Installation

The following commands should be used in order to install the necessary packages on a Fedora Linux system:

1) Install **Java** (required for Spark):
   ```bash
   sudo dnf install java-11-openjdk

2) Install **PySpark**:
    ```bash
   pip install pyspark
    
3) Install **SQLite**:
    ```bash
   sudo dnf install sqlite

4) Install **SQLite3 Python Module**:
     ```bash
   sudo dnf install python3-sqlite

### Setting up and Running PySpark

Start **PySpark**:
```
  pyspark
```

Next, establish a connection to **SQLite** and set up a cursor:
  ```
import sqlite3
```
Define path to SQLite database:
```
path = '/your/path/to/data.db'
conn = sqlite3.connect(path)
cursor = conn.cursor()
```
Create table for subscribers if it doesn't exist:
```
cursor.execute('''create table if not exists subscribers (row_key text primary key,sub_id text, act_dt text)''')
```
Commit and close the connection:
```
conn.commit()
conn.close()
```
### Load Subscriber Data and Clean it
1. Read the subscribers CSV file into a DataFrame:
```
from pyspark.sql.functions import col,lit,date_format
path1='/your/path/to/subscribers.csv'
df1 = spark.read.csv(path1, header=False, inferSchema=True)
df1.show()
```
The columns of df1 have names automatically assigned (_c0, _c1).

2. Rename columns to make them more meaningful:
```
df1= df1.withColumnRenamed('_c0', 'sub_id').withColumnRenamed('_c1', 'act_dt')
df1.show()
```

3. Convert the 'act_dt' column to a proper date format:
```
from pyspark.sql.functions import to_date
df1= df1.withColumn('act_dt', to_date(col('act_dt'), 'yyyy-MM-dd'))
```

4. Remove duplicates and keep only the latest activation date for each sub_id:
 ```
from pyspark.sql.functions import max
df1 = df1.groupBy('sub_id').agg(max('act_dt').alias('act_dt'))
df1.count()                                                       
```
The result is 77 rows instead of 79 that were in the beginning. This means there were duplicate sub_ids.

5a. Create a unique 'row_key' by concatenating sub_id and act_dt:
```
from pyspark.sql.functions import concat_ws
df1 = df1.withColumn('row_key', concat_ws('_', col('sub_id'), date_format(col('act_dt'), 'yyyyMMdd')))
df1.show()
```

5b. Reorder the row_key column to be in first position of the df
```
df1 = df1.select('row_key', 'sub_id', 'act_dt')
df1.show()
```

6. Convert the DataFrame to pandas for SQLite insertion:
```
df1_pandas = df1.toPandas()
conn = sqlite3.connect(path)
df1_pandas.to_sql('subscribers', conn, if_exists='replace', index=False)
conn.close()
```

7. Read from SQLite to verify insertion:
```
conn = sqlite3.connect(path)
cursor = conn.cursor()
import pandas as pd
df_sql = pd.read_sql_query('select * from subscribers', conn)
print(df_sql)
conn.commit()
conn.close()
```

### Load and Clean Transaction Data
1. Read the transaction CSV file into a DataFrame:
```
path2 ='/your/path/to/data_transactions.csv'
df2 = spark.read.csv(path2, header=False, inferSchema=False)
df2.show()
```

2. Rename columns for clarity:
```
df2 = df2.withColumnRenamed('_c0', 'timestamp').withColumnRenamed('_c1', 'sub_id').withColumnRenamed('_c2', 'amount').withColumnRenamed('_c3', 'channel')
df2.show()
```

3. Remove duplicates (if any) and ensure the latest timestamp is kept for each transaction:
```
from pyspark.sql import functions as F
from pyspark.sql.window import Window
```

##### Step 1: Modify the other columns based on timestamp condition
There is a row that its values are correct, but in the wrong columns
```
df2 = df2.withColumn('sub_id', F.when(df2.timestamp == 459991, 459991).otherwise(df2.sub_id)).withColumn('amount', F.when(df2.timestamp == 459991, 0.47).otherwise(df2.amount)).withColumn('channel', F.when(df2.timestamp == 459991, 'USSD').otherwise(df2.channel))
```

##### Step 2: Apply the change to the timestamp column last
Set timestamp to None where timestamp == 459991:
```
df2 = df2.withColumn('timestamp', F.when(df2.timestamp == 459991, None).otherwise(df2.timestamp))
```

4. Calculate the mode of timestamp and replace NULL values where necessary:
```
mode_timestamp = df2.groupBy('timestamp').count().orderBy(F.desc('count')).first()[0]
df2 = df2.withColumn('timestamp', F.when((F.col('sub_id') == 459991) & (F.col('timestamp').isNull()), mode_timestamp).otherwise(F.col('timestamp')))
df2 = df2.withColumn('timestamp', F.when(df2.sub_id == 654322, mode_timestamp).otherwise(df2.timestamp))
```

Ensure timestamp is in correct format
```
df2 = df2.withColumn("timestamp", col("timestamp").cast("timestamp")) 
```

Define window specification for latest entry by sub_id
```
window_spec = Window.partitionBy("sub_id").orderBy(F.col("timestamp").desc())
```

Add row number and filter to keep the latest record for each sub_id
```
df2 = df2.withColumn("row_num", F.row_number().over(window_spec))
```
Filter to keep only the latest row for each 'sub_id' (where row_num == 1)
```
df2 = df2.filter(F.col("row_num") == 1).drop("row_num")
```

Now, df2 will have only the distinct 'sub_id' with the latest 'timestamp'
(It did not have any duplicates afterall)

5. Calculate the mean amount for replacing invalid amounts:
```
mean_amount = df2.filter(df2.amount.isNotNull()).agg(F.mean('amount')).collect()[0][0]
```

Replace invalid amount with the mean value
```
df2 = df2.withColumn('amount', F.when(df2.amount == '###', mean_amount).otherwise(df2.amount))
```
Cast amount to decimal type
```
from pyspark.sql.types import DecimalType
df2 = df2.withColumn('amount', df2['amount'].cast(DecimalType(10, 4)))
df2.show()
```

6.Fix invalid channel values:
```
valid_channel = df2.filter(df2.channel != '***')
mode_channel = valid_channel.groupBy('channel').count().orderBy(F.desc('count')).first()

df2 = df2.withColumn('channel', F.when((df2.channel == '***') | (df2.channel.isNull()), F.lit(mode_channel['channel'] if mode_channel else 'Unknown')).otherwise(df2.channel))
df2.show()
```

### Join DataFrames
1. Perform a left join to find unmatched subscribers:
```
unmatched_df = df2.join(df1, on='sub_id', how='left').filter(df1.sub_id.isNull())
```
Save unmatched subscribers to Parquet:
```
output_path = "/home/ele/Documents/RE__Junior_Data_Engineer_Assignment__1_/unmatched_subscribers.parquet"
unmatched_df.write.mode("overwrite").parquet(output_path)
```
Perform an inner join to get matched rows:
```
df2_filtered = df2.join(df1, on='sub_id', how='inner')
df2_filtered.count()
```
The result is 77 rows, the same with df1, because we removed the unmatched sub_ids.

3. Rename 'row_key' column to 'sub_row_key':
```
df2_filtered = df2_filtered.withColumnRenamed('row_key', 'sub_row_key')
```

### Create Transactions Table in SQLite
1. Create the transactions table in the SQLite database:
```
conn = sqlite3.connect(path)
cursor = conn.cursor()
cursor.execute('''create table if not exists transactions (sub_id text, timestamp timestamp, amount decimal(10,4), channel text, sub_row_key text, act_dt text, primary key (sub_id, timestamp)''')
conn.commit()
```

### Insert Data into Transactions Table
1. Convert the DataFrame to pandas and insert the data into the SQLite database:
```
df2_pandas = df2_filtered.toPandas()
df2_pandas['amount'] = df2_pandas['amount'].astype(float)
df2_pandas.to_sql('transactions', conn, if_exists='replace', index=False)
```
Round the amount to 4 decimal places:
```
df2_pandas['amount'] = df2_pandas['amount'].round(4)
```
2. Close the database connection:
```
conn.close()
```
That's it! The data has been cleaned, transformed, and saved into SQLite and Parquet formats.
