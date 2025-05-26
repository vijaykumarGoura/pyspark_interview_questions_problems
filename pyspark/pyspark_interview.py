from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[1]").appName("pension_class").getOrCreate()
my_list=[(1,"venkatamma",60),(2,"subbamma",55),(3,"achamma",45),(4,"Ramulamma",49)]
schema=StructType([StructField("id",IntegerType(),True),
                   StructField("name",StringType(),True),
                   StructField("age",IntegerType(),True)])
print(type(schema))


try:
    df = spark.createDataFrame(data=my_list,schema=schema)
    print("Inside try block and the code looks good and executing the logic")
    df.show()
except Exception:
    print("Something wrong with Df creating")
	



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Initialize Spark session


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("CamelCaseTransformation").getOrCreate()
my_list = [(1, "venkatamma", 60), (2, "subbamma", 55), (3, "achamma", 45), (4, "Ramulamma", 49)]

# Define schema
columns = ["id", "name", "age"]

df_camel = spark.createDataFrame(my_list, columns)

df_camel = df_camel.withColumn("name",initcap(col("name")))
df_camel.show()
spark = SparkSession.builder.appName("CamelCaseTransformation").getOrCreate()
my_list = [(1, "venkatamma", 60), (2, "subbamma", 55), (3, "achamma", 45), (4, "Ramulamma", 49)]

# Define schema
columns = ["id", "name", "age"]

df_camel = spark.createDataFrame(my_list, columns)

def to_camel_case(name):
    return name.capitalize() if name else None
camel_case_udf = udf(to_camel_case,StringType())
df_camel = df_camel.withColumn("name",camel_case_udf(col("name")))
df_camel.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("CamelCaseTransformation").getOrCreate()
my_list = [(1, "venkatamma", 60), (2, "subbamma", 55), (3, "achamma", 45), (4, "Ramulamma", 49)]

# Define schema
columns = ["id", "name", "age"]

df_camel = spark.createDataFrame(my_list, columns)

df_camel = df_camel.withColumn("name",initcap(col("name")))
df_camel.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("CamelCaseTransformation").getOrCreate()

drop_dups = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Alice", 25)]

columns = ["name","age"]

df_dups = spark.createDataFrame(drop_dups,columns)

df_dups = df_dups.distinct()

df_dups.show()


df_null.show()



from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.appName("Remove nulls").getOrCreate()

# Sample data with NULL values
data = [
    (1, "venkatamma", 60),
    (2, None, 55),         # Row with NULL value in 'name'
    (3, "achamma", None),  # Row with NULL value in 'age'
    (4, "Ramulamma", 49),
    (5, None, None)        # Row with multiple NULLs
]

# Define column names
columns = ["id", "name", "age"]

# Create DataFrame
df_null = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df_null.show()

df_null =df_null.dropna()
print("DataFrame after null removed:")
df_null.show()

df_null =df_null.dropna(how="any")
print("DataFrame after dropna(how=ALL) removed:")
df_null.show()


df_null =df_null.dropna(subset=["name"])
print("DataFrame after dropna(subset=name) removed:")
df_null.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("FilterYoungOld").getOrCreate()

# Sample data
data = [
    (1, "Venkatamma", 60),
    (2, "Subbamma", 55),
    (3, "Achamma", 45),
    (4, "Ramulamma", 49),
    (5, "Krishna", 25),
    (6, "Sita", 30)
]

# Define column names
columns = ["id", "name", "age"]

df_filter = spark.createDataFrame(data,columns)
category_df = df_filter.filter(col("age")<50)
category_df.show()
old_df = df_filter.filter(col("age")>50)
old_df.show()

df_with_column = spark.createDataFrame(data,columns)

df_with_column = df_with_column.withColumn("catefory",when(col("age")<50,"young").otherwise("old"))
df_with_column.show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("FilterByAge").getOrCreate()
data = [("Product A", 100), ("Product B", 200), ("Product C", 150)]
columns = ["product", "price"]
discount_df = spark.createDataFrame(data,columns)
discount_df.show()

discount_df = discount_df.select(col("product"),col("price"),when(col("price")> 150,col("price")*0.9).otherwise(col("price")))
discount_df = discount_df.orderBy(col("price").desc())
discount_df.show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("AliasExample").getOrCreate()
# Sample DataFrame
data = [(1, "Alice"), (2, "Bob")]
alias_df = spark.createDataFrame(data,["id","name"])
alias_df.show()

alias_df = alias_df.select(col("id"),col("name").alias("userName"))
alias_df.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("FilterYoungOld").getOrCreate()

# Sample data
data = [
    (1, "Venkatamma", 60),
    (2, "Subbamma", 55),
    (3, "Achamma", 45),
    (4, "Ramulamma", 49),
    (5, "Krishna", 25),
    (6, "Sita", 30)
]

# Define column names
columns = ["id", "name", "age"]

startsWIthDf = spark.createDataFrame(data,columns)
startsWIthDf.show()

startsWIthDf = startsWIthDf.select(col("id"),col("name"),when((col("age")> 40) & (col("name").endswith("a")),"Senior").otherwise("Junior").alias("category"))
startsWIthDf.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, substring

# Initialize Spark Session
spark = SparkSession.builder.appName("StringManipulation").getOrCreate()

# Sample DataFrame
data = [("Alice", "abc123"), ("Bob", "def456"), ("Charlie", "ghi789")]
str_manipulation_df = spark.createDataFrame(data,["name","code"])
str_manipulation_df.show()
str_manipulation_df = str_manipulation_df.select(col("name"),col("code"),
                                                 when(col("code").rlike("[0-9]{3}"),substring(col("name"),1,3)).alias("code after string manipulation"))
str_manipulation_df.show()


from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.appName("Remove nulls").getOrCreate()

# Sample data with NULL values
data = [
    (1, "venkatamma", 60),
    (2, None, 55),         # Row with NULL value in 'name'
    (3, "achamma", None),  # Row with NULL value in 'age'
    (4, "Ramulamma", 49),
    (5, None, None)        # Row with multiple NULLs
]

# Define column names
columns = ["id", "name", "age"]

# Create DataFrame
df_null = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df_null.show()

df_null =df_null.dropna()
print("DataFrame after null removed:")
df_null.show()

df_null =df_null.dropna(how="any")
print("DataFrame after dropna(how=ALL) removed:")
df_null.show()


df_null =df_null.dropna(subset=["name"])
print("DataFrame after dropna(subset=name) removed:")
df_null.show()

filter_not_null = df_null.filter(col("name").isNotNull())
filter_not_null.show()

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.appName("filter based on condition").getOrCreate()

data = [("Alice", 25, 1500), ("Bob", 30, 2000), ("Charlie", 35, 3000)]
columns = ["Name","age","salary"]
df_cnd = spark.createDataFrame(data,columns)
df_cnd.show()
df_cnd= df_cnd.filter((col("age")>35) & (col("salary")>2500) | (col("name") == "Alice"))
df_cnd.show()


from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.appName("Group by and aggregation").getOrCreate()
data =[("Order1", "John", 100),
("Order2", "Alice", 200),
("Order3", "Bob", 150),
("Order4", "Alice", 300),
("Order5", "Bob", 250),
("Order6", "John", 400)]
columns =["OrderID", "Customer", "Amount"]

df_grp= spark.createDataFrame(data,columns)
df_result  = df_grp.groupBy(col("Customer")).agg(count(col("OrderID")).alias("OrdersPlaced"),sum(col("Amount")).alias("Total_amount_by_each_customer"))
df_result.show() 


===========================

INvalid_Transaction -- JPMOrgan

Bank of Ireland has requested that you detect invalid transactions in December 2022. An invalid transaction is one that occurs outside of the bank’s normal business hours. The following are the hours of operation for all branches:

Monday — Friday: 09:00–16:00
Saturday & Sunday: Closed
Irish Public Holidays: 25th and 26th December
You need to find the transaction IDs of all invalid transactions that occurred in December 2022.

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
from pyspark.sql.functions import col, dayofweek, month, year, hour, minute,dayofmonth
spark = SparkSession.builder.appName("Invalid transaction").getOrCreate()
# Sample data
data = [
    (1051, '2022-12-03 10:15'),
    (1052, '2022-12-03 17:00'),
    (1053, '2022-12-04 10:00'),
    (1054, '2022-12-04 14:00'),
    (1055, '2022-12-05 08:59'),
    (1056, '2022-12-05 16:01'),
    (1057, '2022-12-06 09:00'),
    (1058, '2022-12-06 15:59'),
    (1059, '2022-12-07 12:00'),
    (1060, '2022-12-08 09:00'),
    (1061, '2022-12-09 10:00'),
    (1062, '2022-12-10 11:00'),
    (1063, '2022-12-10 17:30'),
    (1064, '2022-12-11 12:00'),
    (1065, '2022-12-12 08:59'),
    (1066, '2022-12-12 16:01'),
    (1067, '2022-12-25 10:00'),
    (1068, '2022-12-25 15:00'),
    (1069, '2022-12-26 09:00'),
    (1070, '2022-12-26 14:00'),
    (1071, '2022-12-26 16:30'),
    (1072, '2022-12-27 09:00'),
    (1073, '2022-12-28 08:30'),
    (1074, '2022-12-29 16:15'),
    (1075, '2022-12-30 14:00'),
    (1076, '2022-12-31 10:00')
]
columns=["transaction_id", "time_stamp"]

df = spark.createDataFrame(data,columns)
#Filter transaction 
df_december_2022 = df.filter((month(col("time_stamp"))==12) & (year(col("time_stamp"))==2022))
df_december_2022.show()
# Check if the transaction was on a weekend (Saturday or Sunday)
weekend_check = (dayofweek(col('time_stamp')).isin([1, 7]))
outside_business_hours = ((hour(col("time_stamp"))<9) | (hour(col("time_stamp"))>16) | (hour(col("time_stamp"))==16) & (hour(col("time_stamp"))>0))
public_holidays= ((dayofmonth(col("time_stamp"))==25) | (dayofmonth(col("time_stamp"))==26))
invalid_transactions = df_december_2022.filter(weekend_check | outside_business_hours | public_holidays)
# Show invalid transaction IDs
invalid_transactions.select("transaction_id").show()

+--------------+
|transaction_id|
+--------------+
|          1051|
|          1052|
|          1053|
|          1054|
|          1055|
|          1056|
|          1062|
|          1063|
|          1064|
|          1065|
|          1066|
|          1067|
|          1068|
|          1069|
|          1070|
|          1071|
|          1073|
|          1074|
|          1076|
+--------------+


====


from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.appName("Read pipe delimiter txt file and create table").getOrCreate()

df_text_file = spark.read.option("delimter","|").option("header",True).csv("dbfs:/FileStore/tables/customer_details.txt")
df_text_file.show()
df_text_file.write.mode("overwrite").saveAsTable("customer_details")


==============

Calculate the lead and lag of the salary column ordered by id


from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.appName("lead and lag of the salary column ordered by id").getOrCreate()

data = [
(1,"karthik",1000),
(2,"Mohan",2000),
(3,"vinay",1500),
(4,"deva",3000)
]
columns =["id","name","salary"]

lead_lag = spark.createDataFrame(data,columns)
lead_lag.show()
window_lead_lag = Window.orderBy(col("salary"))
df = lead_lag.select(col("id"),col("name"),col("salary"),lead(col("salary")).over(window_lead_lag).alias("lead-salary"),lag(col("salary")).over(window_lead_lag).alias("lag-salary"))
df.show()


+---+-------+------+-----------+----------+
| id|   name|salary|lead-salary|lag-salary|
+---+-------+------+-----------+----------+
|  1|karthik|  1000|       2000|      null|
|  2|  Mohan|  2000|       1500|      1000|
|  3|  vinay|  1500|       3000|      2000|
|  4|   deva|  3000|       null|      1500|
+---+-------+------+-----------+----------+

========================

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.appName("total revenue for each user").getOrCreate()
data =[("Product1", "Category1", 100),
      ("Product2", "Category2", 200),
      ("Product3", "Category1", 150),
      ("Product4", "Category3", 300),
      ("Product5", "Category2", 250),
      ("Product6", "Category3", 180)]
columns = ["Product", "Category", "Revenue"]
revenue_df = spark.createDataFrame(data,columns)
revenue_df.show()
window_function = Window.partitionBy(col("Category")).orderBy(col("Revenue"))
df = revenue_df.select(col("Product"),col("Category"),col("Revenue"),sum(col("Revenue")).over(window_function).alias("Total_revenue"))
df.show()

+--------+---------+-------+-------------+
| Product| Category|Revenue|Total_revenue|
+--------+---------+-------+-------------+
|Product1|Category1|    100|          100|
|Product3|Category1|    150|          250|
|Product2|Category2|    200|          200|
|Product5|Category2|    250|          450|
|Product6|Category3|    180|          180|
|Product4|Category3|    300|          480|
+--------+---------+-------+-------------+

=================

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.appName("total revenue for each product").getOrCreate()
data =[("Product1", "Category1", 100),
      ("Product2", "Category2", 200),
      ("Product3", "Category1", 150),
      ("Product4", "Category3", 300),
      ("Product5", "Category2", 250),
      ("Product6", "Category3", 180)]
columns = ["Product", "Category", "Revenue"]
revenue_df = spark.createDataFrame(data,columns)
revenue_df.show()
window_function = Window.partitionBy(col("product")).orderBy(col("product"))
df = revenue_df.select(col("Product"),col("Category"),col("Revenue"),sum(col("Revenue")).over(window_function).alias("Total_revenue"))
df.show()

+--------+---------+-------+-------------+
| Product| Category|Revenue|Total_revenue|
+--------+---------+-------+-------------+
|Product1|Category1|    100|          100|
|Product2|Category2|    200|          200|
|Product3|Category1|    150|          150|
|Product4|Category3|    300|          300|
|Product5|Category2|    250|          250|
|Product6|Category3|    180|          180|
+--------+---------+-------+-------------+]

=========

last 3 rating

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.appName("total revenue for each user").getOrCreate()

data =[
("User1", "Movie1", 4.5),
      ("User1", "Movie2", 3.5),
      ("User1", "Movie3", 2.5),
      ("User1", "Movie4", 4.0),
      ("User1", "Movie5", 3.0),
      ("User1", "Movie6", 4.5),
      ("User2", "Movie1", 3.0),
      ("User2", "Movie2", 4.0),
      ("User2", "Movie3", 4.5),
      ("User2", "Movie4", 3.5),
      ("User2", "Movie5", 4.0),
      ("User2", "Movie6", 3.5)]
columns=["User", "Movie", "Rating"]

df_rating = spark.createDataFrame(data,columns)
window_function = Window.partitionBy(col("User")).orderBy(col("Rating")).rowsBetween(-2,0)
df= df_rating.select(col("User"),col("Movie"),col("Rating"),avg("Rating").over(window_function).alias("avg_rating"))
df.show()

+-----'
+------+------+------------------+
| User| Movie|Rating|        avg_rating|
+-----+------+------+------------------+
|User1|Movie3|   2.5|               2.5|
|User1|Movie5|   3.0|              2.75|
|User1|Movie2|   3.5|               3.0|
|User1|Movie4|   4.0|               3.5|
|User1|Movie1|   4.5|               4.0|
|User1|Movie6|   4.5| 4.333333333333333|
|User2|Movie1|   3.0|               3.0|
|User2|Movie4|   3.5|              3.25|
|User2|Movie6|   3.5|3.3333333333333335|
|User2|Movie2|   4.0|3.6666666666666665|
|User2|Movie5|   4.0|3.8333333333333335|
|User2|Movie3|   4.5| 4.166666666666667|'

==============

First three 

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.appName("total revenue for each user").getOrCreate()

data =[
("User1", "Movie1", 4.5),
      ("User1", "Movie2", 3.5),
      ("User1", "Movie3", 2.5),
      ("User1", "Movie4", 4.0),
      ("User1", "Movie5", 3.0),
      ("User1", "Movie6", 4.5),
      ("User2", "Movie1", 3.0),
      ("User2", "Movie2", 4.0),
      ("User2", "Movie3", 4.5),
      ("User2", "Movie4", 3.5),
      ("User2", "Movie5", 4.0),
      ("User2", "Movie6", 3.5)]
columns=["User", "Movie", "Rating"]

df_rating = spark.createDataFrame(data,columns)
window_function = Window.partitionBy(col("User")).orderBy(col("Rating")).rowsBetween(0,2)
df= df_rating.select(col("User"),col("Movie"),col("Rating"),avg("Rating").over(window_function).alias("avg_rating"))
df.show()

+-----+------+------+------------------+
| User| Movie|Rating|        avg_rating|
+-----+------+------+------------------+
|User1|Movie3|   2.5|               3.0|
|User1|Movie5|   3.0|               3.5|
|User1|Movie2|   3.5|               4.0|
|User1|Movie4|   4.0| 4.333333333333333|
|User1|Movie1|   4.5|               4.5|
|User1|Movie6|   4.5|               4.5|
|User2|Movie1|   3.0|3.3333333333333335|
|User2|Movie4|   3.5|3.6666666666666665|
|User2|Movie6|   3.5|3.8333333333333335|
|User2|Movie2|   4.0| 4.166666666666667|
|User2|Movie5|   4.0|              4.25|
|User2|Movie3|   4.5|               4.5|
+-----+------+------+------------------+    


=============

running balance 

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.appName("Net balance using rows between unbounded preceding and current row").getOrCreate()

data =[(101,"2022-04-25",'credit','Cash',800),
       (101,'2022-04-25','credit','UPI',600),
       (101,'2022-04-28','credit','UPI',200),
       (101,'2022-04-29','debit','Credit Card',300),
       (102,'2022-03-15','credit','UPI',1800),
       (102,'2022-03-20','debit','UPI',200),
       (102,'2022-04-20','debit','Credit Card',700)]

columns = ["custid","txndate","status","mode","amount"]
df = spark.createDataFrame(data,columns)
df.show()

window_func= Window.partitionBy(col("custid")).orderBy(col("txndate")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_cte = df.withColumn("cashback",when(col("mode")=="Credit Card",col("amount")*0.02).otherwise(0)).withColumn("net_balance",when(col("status")=="credit",col("amount")).otherwise(-col("amount")))

#df_result = df.select(col("custid"),col("txndate"),col("status"),col("amount"),col("mode"),
                      #when(col("mode")=="Credit Card"),(col("amount")*0.02))
df_cte.show()

df_result=df_cte.withColumn("Total",sum(col("net_balance")+col("cashback")).over(window_func))

df_result.select("custid","txndate","status","amount","mode","cashback","Total").show()

+------+----------+------+------+-----------+--------+------+
|custid|   txndate|status|amount|       mode|cashback| Total|
+------+----------+------+------+-----------+--------+------+
|   101|2022-04-25|credit|   800|       Cash|     0.0| 800.0|
|   101|2022-04-25|credit|   600|        UPI|     0.0|1400.0|
|   101|2022-04-28|credit|   200|        UPI|     0.0|1600.0|
|   101|2022-04-29| debit|   300|Credit Card|     6.0|1306.0|
|   102|2022-03-15|credit|  1800|        UPI|     0.0|1800.0|
|   102|2022-03-20| debit|   200|        UPI|     0.0|1600.0|
|   102|2022-04-20| debit|   700|Credit Card|    14.0| 914.0|
+------+----------+------+------+-----------+--------+------+

==================

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.appName("lead and lag of the salary column ordered by id").getOrCreate()
data =[
       (1,"kitkat",1000.0,"2021-01-01"),
       (2,"kitkat",2000.0,"2021-01-02"),
       (3,"kitkat",1000.0,"2021-01-03"),
       (4,"kitkat",2000.0,"2021-01-04"),
       (5,"kitkat",3000.0,"2021-01-05"),
       (6,"kitkat",1000.0,"2021-01-06")]

columns=["id","name","price","date"]

df = spark.createDataFrame(data,columns)

window_spec=Window.orderBy(col("date"))

df=df.select(col("id"),col("name"),col("price"),col("date"),lag(col("price")).over(window_spec).alias("lag_result"),(col("price")-(lag(col("price")).over(window_spec))).alias("price-lag_price"))
df.printSchema()
df.show()


+---+------+------+----------+----------+---------------+
| id|  name| price|      date|lag_result|price-lag_price|
+---+------+------+----------+----------+---------------+
|  1|kitkat|1000.0|2021-01-01|      null|           null|
|  2|kitkat|2000.0|2021-01-02|    1000.0|         1000.0|
|  3|kitkat|1000.0|2021-01-03|    2000.0|        -1000.0|
|  4|kitkat|2000.0|2021-01-04|    1000.0|         1000.0|
|  5|kitkat|3000.0|2021-01-05|    2000.0|         1000.0|
|  6|kitkat|1000.0|2021-01-06|    3000.0|        -2000.0|
+---+------+------+----------+----------+---------------+

=============================

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.appName("Train Platform calculation").getOrCreate()
arrivals_data = [
    (1, '2024-11-17 08:00'),
    (2, '2024-11-17 08:05'),
    (3, '2024-11-17 08:05'),
    (4, '2024-11-17 08:10'),
    (5, '2024-11-17 08:10'),
    (6, '2024-11-17 12:15'),
    (7, '2024-11-17 12:20'),
    (8, '2024-11-17 12:25'),
    (9, '2024-11-17 15:00'),
    (10, '2024-11-17 15:00'),
    (11, '2024-11-17 15:00'),
    (12, '2024-11-17 15:06'),
    (13, '2024-11-17 20:00'),
    (14, '2024-11-17 20:10')
]

departures_data = [
    (1, '2024-11-17 08:15'),
    (2, '2024-11-17 08:10'),
    (3, '2024-11-17 08:20'),
    (4, '2024-11-17 08:25'),
    (5, '2024-11-17 08:20'),
    (6, '2024-11-17 13:00'),
    (7, '2024-11-17 12:25'),
    (8, '2024-11-17 12:30'),
    (9, '2024-11-17 15:05'),
    (10, '2024-11-17 15:10'),
    (11, '2024-11-17 15:15'),
    (12, '2024-11-17 15:15'),
    (13, '2024-11-17 20:15'),
    (14, '2024-11-17 20:15')
]

arrival_schema =StructType([
    StructField("trainId",IntegerType(),True),
    StructField("arrival_time",StringType(),True)
])
departure_schema =StructType([
    StructField("trainId",IntegerType(),True),
    StructField("departure_time",StringType(),True)
])
arrivals_df=spark.createDataFrame(arrivals_data,arrival_schema)
departure_df=spark.createDataFrame(departures_data,arrival_schema)
arrivals_df.printSchema()
departure_df.printSchema()

# Convert the time strings to timestamps for easier handling
arrivals_df = arrivals_df.withColumn("arrival_time",col("arrival_time").cast("timestamp"))
departure_df = departure_df.withColumn("departure_time",col("arrival_time").cast("timestamp"))
#after converting to timestamp
arrivals_df.printSchema()
departure_df.printSchema()

# Add event type (arrival = 1, departure = -1)
arrivals_df= arrivals_df.withColumn("event_type",lit(1))
departure_df= departure_df.withColumn("event_type",lit(-1))
arrivals_df.printSchema()
departure_df.printSchema()
# union 
arrival_union_df = arrivals_df.select(col("trainId"),col("arrival_time"),col("event_type")).withColumnRenamed("arrival_time", "event_time")
arrival_union_df.show()
departure_union_df = departure_df.select(col("trainId"),col("departure_time"),col("event_type")).withColumnRenamed("departure_time","event_time")
departure_union_df.show()
# Union both DataFrames into one, marking events as either arrival or departure
all_events_df = arrival_union_df.union(departure_union_df)
# Sort events by event_time and prioritize arrivals over departures at the same time
all_events_df = all_events_df.orderBy('event_time', col('event_type').desc())
# Step 2: Use a window function to calculate the running total of platforms needed at each time
window_spec = Window.orderBy(col("event_time"),col("event_type").desc())
# Calculate running sum of platforms needed
all_events_df = all_events_df.withColumn("platforms_needed",sum("event_type").over(window_spec))
# Step 3: Find the maximum platforms_needed at any given time
max_platforms = all_events_df.agg(max('platforms_needed')).collect()[0][0]

# Output the result
print(f"The minimum number of platforms required: {max_platforms}")


+-------+-------------------+----------+
|trainId|         event_time|event_type|
+-------+-------------------+----------+
|      1|2024-11-17 08:00:00|         1|
|      2|2024-11-17 08:05:00|         1|
|      3|2024-11-17 08:05:00|         1|
|      4|2024-11-17 08:10:00|         1|
|      5|2024-11-17 08:10:00|         1|
|      6|2024-11-17 12:15:00|         1|
|      7|2024-11-17 12:20:00|         1|
|      8|2024-11-17 12:25:00|         1|
|      9|2024-11-17 15:00:00|         1|
|     10|2024-11-17 15:00:00|         1|
|     11|2024-11-17 15:00:00|         1|
|     12|2024-11-17 15:06:00|         1|
|     13|2024-11-17 20:00:00|         1|
|     14|2024-11-17 20:10:00|         1|
+-------+-------------------+----------+

+-------+-------------------+----------+
|trainId|         event_time|event_type|
+-------+-------------------+----------+
|      1|2024-11-17 08:15:00|        -1|
|      2|2024-11-17 08:10:00|        -1|
|      3|2024-11-17 08:20:00|        -1|
|      4|2024-11-17 08:25:00|        -1|
|      5|2024-11-17 08:20:00|        -1|
|      6|2024-11-17 13:00:00|        -1|
|      7|2024-11-17 12:25:00|        -1|
|      8|2024-11-17 12:30:00|        -1|
|      9|2024-11-17 15:05:00|        -1|
|     10|2024-11-17 15:10:00|        -1|
|     11|2024-11-17 15:15:00|        -1|
|     12|2024-11-17 15:15:00|        -1|
|     13|2024-11-17 20:15:00|        -1|
|     14|2024-11-17 20:15:00|        -1|
+-------+-------------------+----------+

The minimum number of platforms required: 5

=========================

Problem Overview
We have two datasets:

Sessions Table: Contains records of when users started their sessions.
Order Summary Table: Contains records of orders placed by users along with their values.
We want to:
Find users who started a session and placed an order on the same day.
Calculate the total number of orders and the total order value for those users.


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum

# Initialize Spark session
spark = SparkSession.builder.appName("SessionOrderAnalysis").getOrCreate()

# Sample data for sessions
sessions_data = [
    (1, 1, '2024-01-01 00:00:00'),
    (2, 2, '2024-01-02 00:00:00'),
    (3, 3, '2024-01-05 00:00:00'),
    (4, 3, '2024-01-05 00:00:00'),
    (5, 4, '2024-01-03 00:00:00'),
    (6, 4, '2024-01-03 00:00:00'),
    (7, 5, '2024-01-04 00:00:00'),
    (8, 5, '2024-01-04 00:00:00'),
    (9, 3, '2024-01-05 00:00:00'),
    (10, 5, '2024-01-04 00:00:00')
]

# Sample data for orders
orders_data = [
    (1, 1, 152, '2024-01-01 00:00:00'),
    (2, 2, 485, '2024-01-02 00:00:00'),
    (3, 3, 398, '2024-01-05 00:00:00'),
    (4, 3, 320, '2024-01-05 00:00:00'),
    (5, 4, 156, '2024-01-03 00:00:00'),
    (6, 4, 121, '2024-01-03 00:00:00'),
    (7, 5, 238, '2024-01-04 00:00:00'),
    (8, 5, 70, '2024-01-04 00:00:00'),
    (9, 3, 152, '2024-01-05 00:00:00'),
    (10, 5, 171, '2024-01-04 00:00:00')
]
sessions_df = spark.createDataFrame(sessions_data,["session_id","user_id","session_date"])
orders_df = spark.createDataFrame(orders_data,["order_id","user_id","order_value","order_date"])
sessions_df.printSchema()
orders_df.printSchema()
# Convert session_date and order_date to date format (ignoring time part)
sessions_df = sessions_df.withColumn("session_date_only",to_date(col("session_date")))
orders_df = orders_df.withColumn("order_date_only",to_date(col("order_date")))
sessions_df.printSchema()
orders_df.printSchema()
join_df = sessions_df.alias("s").join(orders_df.alias("o"),(col("s.user_id")==col("o.user_id")) & (col("s.session_date_only")==col("o.order_date_only")),"inner")

result_df = join_df.groupBy(col("s.user_id"),col("s.session_date_only")).agg(count(col("o.order_id")).alias("Total_orders"),sum(col("o.order_value")).alias("Total_order_value"))
result_df= result_df.filter(col("Total_orders")>0)
result_df.show(truncate=False)

+----------+-------+-------------------+-----------------+--------+-------+-----------+-------------------+---------------+
|session_id|user_id|session_date       |session_date_only|order_id|user_id|order_value|order_date         |order_date_only|
+----------+-------+-------------------+-----------------+--------+-------+-----------+-------------------+---------------+
|1         |1      |2024-01-01 00:00:00|2024-01-01       |1       |1      |152        |2024-01-01 00:00:00|2024-01-01     |
|2         |2      |2024-01-02 00:00:00|2024-01-02       |2       |2      |485        |2024-01-02 00:00:00|2024-01-02     |
|3         |3      |2024-01-05 00:00:00|2024-01-05       |3       |3      |398        |2024-01-05 00:00:00|2024-01-05     |
|3         |3      |2024-01-05 00:00:00|2024-01-05       |4       |3      |320        |2024-01-05 00:00:00|2024-01-05     |
|3         |3      |2024-01-05 00:00:00|2024-01-05       |9       |3      |152        |2024-01-05 00:00:00|2024-01-05     |
|4         |3      |2024-01-05 00:00:00|2024-01-05       |3       |3      |398        |2024-01-05 00:00:00|2024-01-05     |
|4         |3      |2024-01-05 00:00:00|2024-01-05       |4       |3      |320        |2024-01-05 00:00:00|2024-01-05     |
|4         |3      |2024-01-05 00:00:00|2024-01-05       |9       |3      |152        |2024-01-05 00:00:00|2024-01-05     |
|9         |3      |2024-01-05 00:00:00|2024-01-05       |3       |3      |398        |2024-01-05 00:00:00|2024-01-05     |
|9         |3      |2024-01-05 00:00:00|2024-01-05       |4       |3      |320        |2024-01-05 00:00:00|2024-01-05     |
|9         |3      |2024-01-05 00:00:00|2024-01-05       |9       |3      |152        |2024-01-05 00:00:00|2024-01-05     |
|5         |4      |2024-01-03 00:00:00|2024-01-03       |5       |4      |156        |2024-01-03 00:00:00|2024-01-03     |
|5         |4      |2024-01-03 00:00:00|2024-01-03       |6       |4      |121        |2024-01-03 00:00:00|2024-01-03     |
|6         |4      |2024-01-03 00:00:00|2024-01-03       |5       |4      |156        |2024-01-03 00:00:00|2024-01-03     |
|6         |4      |2024-01-03 00:00:00|2024-01-03       |6       |4      |121        |2024-01-03 00:00:00|2024-01-03     |
|7         |5      |2024-01-04 00:00:00|2024-01-04       |7       |5      |238        |2024-01-04 00:00:00|2024-01-04     |
|7         |5      |2024-01-04 00:00:00|2024-01-04       |8       |5      |70         |2024-01-04 00:00:00|2024-01-04     |
|7         |5      |2024-01-04 00:00:00|2024-01-04       |10      |5      |171        |2024-01-04 00:00:00|2024-01-04     |
|8         |5      |2024-01-04 00:00:00|2024-01-04       |7       |5      |238        |2024-01-04 00:00:00|2024-01-04     |
|8         |5      |2024-01-04 00:00:00|2024-01-04       |8       |5      |70         |2024-01-04 00:00:00|2024-01-04     |
|8         |5      |2024-01-04 00:00:00|2024-01-04       |10      |5      |171        |2024-01-04 00:00:00|2024-01-04     |
|10        |5      |2024-01-04 00:00:00|2024-01-04       |7       |5      |238        |2024-01-04 00:00:00|2024-01-04     |
|10        |5      |2024-01-04 00:00:00|2024-01-04       |8       |5      |70         |2024-01-04 00:00:00|2024-01-04     |
|10        |5      |2024-01-04 00:00:00|2024-01-04       |10      |5      |171        |2024-01-04 00:00:00|2024-01-04     |
+----------+-------+-------------------+-----------------+--------+-------+-----------+-------------------+---------------+


+-------+-----------------+------------+-----------------+
|user_id|session_date_only|Total_orders|Total_order_value|
+-------+-----------------+------------+-----------------+
|1      |2024-01-01       |1           |152              |
|2      |2024-01-02       |1           |485              |
|3      |2024-01-05       |9           |2610             |
|4      |2024-01-03       |4           |554              |
|5      |2024-01-04       |9           |1437             |
+-------+-----------------+------------+-----------------+


=========================

Problem Statement: You have two DataFrames: one containing employee information (employees) and another containing department information (departments). Write a PySpark function to join these DataFrames on the department ID and fill any missing salary values with the average salary of the respective department.

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, coalesce, round

# Initialize Spark session
spark = SparkSession.builder.appName("Join DataFrames and Fill Missing Values").getOrCreate()
# data for employees
employee_data = [
    (1, "Alice", 70000, 10),
    (2, "Bob", None, 20),  # Missing salary
    (3, "Charlie", 80000, 10),
    (4, "David", None, 30),  # Missing salary
    (5, "Eve", 75000, 20),
    (6, "Frank", 90000, 10),
    (7, "Grace", 52000, 30),
    (8, "Hannah", 62000, 20),
    (9, "Isaac", None, 30),  # Missing salary
    (10, "Jack", 71000, 20)
]

# data for departments
department_data = [
    (10, "Engineering"),
    (20, "HR"),
    (30, "Marketing")
]

employee_df = spark.createDataFrame(employee_data,["employee_id","name","salary","department_id"])
department_df = spark.createDataFrame(department_data,["department_id","department_name"])
employee_df.printSchema()
department_df.printSchema()
average_salary = employee_df.groupBy(col("department_id")).agg(round(avg(col("salary")),2).alias("average_salary"))
average_salary.show()
join_df= employee_df.join(department_df,"department_id","left").join(average_salary,"department_id","left")

fill_missing_salary = join_df.withColumn("salary",coalesce(col("salary"),col("average_salary"))).select(col("employee_id"),col("name"),col("salary"),col("department_name"))
fill_missing_salary.show()

fill_salary = join_df.withColumn("salary",coalesce(join_df.salary,join_df.average_salary)).select(col("employee_id"),col("name"),col("salary"),col("department_name"))
fill_salary.show()


=======================================


Problem Statement
We have to find the 3rd highest total transaction amount from the records. We have two tables: one containing customer details (customers) and the other storing transaction data (card_orders).

Our goal is to:
Retrieve the customer who ranks third in terms of total transaction amount.

Solution
To solve this problem using PySpark, we can break it down into 3 main steps:

Aggregate the data: Join the customers and card_orders tables, then group the data by customer ID to calculate the total transaction amount for each customer.
Rank the customers: Use the rank() function to assign a rank to each customer based on their total transaction amount, in descending order.
Filter the third-highest: Finally, filter the ranked data to retrieve the customer with the rank of 3 (i.e. the third-highest transaction total).


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize the Spark session
spark = SparkSession.builder.master("local").appName("CustomerTransactions").getOrCreate()

# Create the customers DataFrame
customers_data = [
    (1, 'Jill', 'Doe', 'New York', '123 Main St', '555-1234'),
    (2, 'Henry', 'Smith', 'Los Angeles', '456 Oak Ave', '555-5678'),
    (3, 'William', 'Johnson', 'Chicago', '789 Pine Rd', '555-8765'),
    (4, 'Emma', 'Daniel', 'Houston', '321 Maple Dr', '555-4321'),
    (5, 'Charlie', 'Davis', 'Phoenix', '654 Elm St', '555-6789')
]

customers_columns = ['id', 'first_name', 'last_name', 'city', 'address', 'phone_number']

customers_df = spark.createDataFrame(customers_data, customers_columns)

# Create the card_orders DataFrame
card_orders_data = [
    (1, 1, '2024-11-01 10:00:00', 'Electronics', 200),
    (2, 2, '2024-11-02 11:30:00', 'Groceries', 150),
    (3, 1, '2024-11-03 15:45:00', 'Clothing', 120),
    (4, 3, '2024-11-04 09:10:00', 'Books', 90),
    (8, 3, '2024-11-08 10:20:00', 'Groceries', 130),
    (9, 1, '2024-11-09 12:00:00', 'Books', 180),
    (10, 4, '2024-11-10 11:15:00', 'Electronics', 200),
    (11, 5, '2024-11-11 14:45:00', 'Furniture', 150),
    (12, 2, '2024-11-12 09:30:00', 'Furniture', 180)
]

card_orders_columns = ['order_id', 'cust_id', 'order_date', 'order_details', 'total_order_cost']

card_orders_df = spark.createDataFrame(card_orders_data, card_orders_columns)
customer_transaction = customers_df.alias("cust").join(card_orders_df.alias("cord"),col("cust.id") == col("cord.cust_id"),"inner").groupBy(col("cust.id"),col("cust.first_name"),col("cust.last_name")).agg(sum(col("cord.total_order_cost")).alias("total_transaction"))
window_spec = Window.orderBy(desc("total_transaction"))

rank_transactions = customer_transaction.withColumn("rank",rank().over(window_spec))
higest_3rd_salary = rank_transactions.filter(rank_transactions.rank==3).select(col("id"),col("first_name"),col("last_name"))
higest_3rd_salary.show()

+---+----------+---------+
| id|first_name|last_name|
+---+----------+---------+
|  3|   William|  Johnson|

========================================

Problem Statement
We have a dataset of LinkedIn users, where each record contains details about their work history — employer, job position, and the start and end dates of each job.

We want to find out how many users had Microsoft as their employer, and immediately after that, they started working at Google, with no other employers between these two positions.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("LinkedInUsers").getOrCreate()

# Sample data for LinkedIn users
linkedin_data = [
    (1, 'Microsoft', 'developer', '2020-04-13', '2021-11-01'),
    (1, 'Google', 'developer', '2021-11-01', None),
    (2, 'Google', 'manager', '2021-01-01', '2021-01-11'),
    (2, 'Microsoft', 'manager', '2021-01-11', None),
    (3, 'Microsoft', 'analyst', '2019-03-15', '2020-07-24'),
    (3, 'Amazon', 'analyst', '2020-08-01', '2020-11-01'),
    (3, 'Google', 'senior analyst', '2020-11-01', '2021-03-04'),
    (4, 'Google', 'junior developer', '2018-06-01', '2021-11-01'),
    (4, 'Google', 'senior developer', '2021-11-01', None),
    (5, 'Microsoft', 'manager', '2017-09-26', None),
    (6, 'Google', 'CEO', '2015-10-02', None)
]

# Define the schema for the LinkedIn data
linkedin_columns = ['user_id', 'employer', 'position', 'start_date', 'end_date']
linkedIn_df = spark.createDataFrame(linkedin_data,linkedin_columns)
#Sort the data on user_id, start_date
window_spec = Window.partitionBy(col("user_id")).orderBy(col("start_date"))
linkedIn_next_employer = linkedIn_df.withColumn("next_employer",lead(col("employer")).over(window_spec))
result = linkedIn_next_employer.filter((linkedIn_next_employer.employer =="Microsoft") & (linkedIn_next_employer.next_employer=="Google"))

user_ids= result.select(col("user_id"))
user_ids.show()
user_ids= result.select("*")
user_ids.count()

+-------+---------+----------------+----------+----------+-------------+
|      1|Microsoft|       developer|2020-04-13|2021-11-01|       Google|
|      1|   Google|       developer|2021-11-01|      null|         null|
|      2|   Google|         manager|2021-01-01|2021-01-11|    Microsoft|
|      2|Microsoft|         manager|2021-01-11|      null|         null|
|      3|Microsoft|         analyst|2019-03-15|2020-07-24|       Amazon|
|      3|   Amazon|         analyst|2020-08-01|2020-11-01|       Google|
|      3|   Google|  senior analyst|2020-11-01|2021-03-04|         null|
|      4|   Google|junior developer|2018-06-01|2021-11-01|       Google|
|      4|   Google|senior developer|2021-11-01|      null|         null|
|      5|Microsoft|         manager|2017-09-26|      null|         null|
|      6|   Google|             CEO|2015-10-02|      null|         null|
+-------+---------+----------------+----------+----------+-------------+

+-------+---------+---------+----------+----------+-------------+
|user_id| employer| position|start_date|  end_date|next_employer|
+-------+---------+---------+----------+----------+-------------+
|      1|Microsoft|developer|2020-04-13|2021-11-01|       Google|
+-------+---------+---------+----------+----------+-------------+



STeps 
1. sort the dataframe with user_id and start_date uisng window function 
2. Apply Lead function to get the next employer details 
3. filter the dataframe with employer and next_empoyer (microsoft) and (Google)
4. show the result

======================================

Problem Statement
You are given a table with titles of recipes from a cookbook and their page numbers. Your task is to produce a table that represents how the recipes are distributed across the pages of the cookbook. Specifically, for each even-numbered page (the left page), show the title of that page in one column, and in the next column, show the title of the next odd-numbered page (the right page).

Each row should contain:

left_page_number: The page number for the left side (even page).
left_title: The title of the recipe on the left page.
right_title: The title of the recipe on the adjacent right page.
If a page does not contain a recipe, the title should be NULL. Page 0 is guaranteed to be empty, so it will not appear in the result.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
# Initialize Spark session
spark = SparkSession.builder.appName("CookbookTitles").getOrCreate()

# Sample data for cookbook_titles
titles_data = [
    (1, 'Scrambled eggs'),
    (2, 'Fondue'),
    (3, 'Sandwich'),
    (4, 'Tomato soup'),
    (6, 'Liver'),
    (11, 'Fried duck'),
    (12, 'Boiled duck'),
    (15, 'Baked chicken')
]

# Define schema for the cookbook_titles data
titles_columns = ["page_number", "title"]

menu_df = spark.createDataFrame(titles_data,titles_columns)
menu_df.show()
left_page = menu_df.filter(col("page_number") % 2 == 0).select(col("page_number"),col("title"))
right_page = menu_df.filter(col("page_number") % 2 != 0).select(col("page_number"),col("title"))
print("before column rename")
left_page.show()
right_page.show()
left_page= left_page.withColumnRenamed("page_number","left_page_number").withColumnRenamed("title","left_title")
right_page= right_page.withColumnRenamed("page_number","right_page_number").withColumnRenamed("title","right_title")
print("after column renamed")
left_page.show()
right_page.show()

result_df = left_page.join(right_page,left_page.left_page_number+1==right_page.right_page_number,"left").select(col("left_page_number"),"left_title","right_title")
result_df.show()


+----------------+-----------+-----------+
|left_page_number| left_title|right_title|
+----------------+-----------+-----------+
|               2|     Fondue|   Sandwich|
|               4|Tomato soup|       null|
|               6|      Liver|       null|
|              12|Boiled duck|       null|
+----------------+-----------+-----------+


steps 
1. created two dataframes for even and odd using filter
2. two dataframes columns renamed as per the even and odd using withColumnRenamed
3. Joined both the dataframes as left join with left_page_number +1 
4. displayed the results.

========================================
Problem Statement:
Given a dataset of employee details, find the highest salary that appears only once.

Our output should display the highest salary among the salaries that appear exactly once in the dataset.


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Find Highest Unique Salary").getOrCreate()

# Sample data for 'employee'
data = [
    (5, 'Max', 'George', 26, 'M', 'Sales', 'Sales', 1300, 200, 150, 'Max@company.com', 'California', '2638 Richards Avenue', 1),
    (13, 'Katty', 'Bond', 56, 'F', 'Manager', 'Management', 150000, 0, 300, 'Katty@company.com', 'Arizona', None, 1),
    (11, 'Richerd', 'Gear', 57, 'M', 'Manager', 'Management', 250000, 0, 300, 'Richerd@company.com', 'Alabama', None, 1),
    (10, 'Jennifer', 'Dion', 34, 'F', 'Sales', 'Sales', 1000, 200, 150, 'Jennifer@company.com', 'Alabama', None, 13),
    (19, 'George', 'Joe', 50, 'M', 'Manager', 'Management', 250000, 0, 300, 'George@company.com', 'Florida', '1003 Wyatt Street', 1),
    (18, 'Laila', 'Mark', 26, 'F', 'Sales', 'Sales', 1000, 200, 150, 'Laila@company.com', 'Florida', '3655 Spirit Drive', 11),
    (20, 'Sarrah', 'Bicky', 31, 'F', 'Senior Sales', 'Sales', 2000, 200, 150, 'Sarrah@company.com', 'Florida', '1176 Tyler Avenue', 19)
]

columns = ["id", "first_name", "last_name", "age", "sex", "employee_title", "department", "salary", "target", "bonus", "email", "city", "address", "manager_id"]

highest_salary_df = spark.createDataFrame(data,columns)
highest_salary_df.show()
group_salary = highest_salary_df.groupBy(col("salary")).agg(count("salary").alias("salary_count"))
group_salary.show()
result = group_salary.filter(col("salary_count")==1).agg(max(col("salary")))
result.show()


+------+------------+
|salary|salary_count|
+------+------------+
|  1300|           1|
|150000|           1|
|250000|           2|
|  1000|           2|
|  2000|           1|
+------+------------+

+-----------+
|max(salary)|
+-----------+
|     150000|
+-----------+

=============================
Broadcast 
---------
from pyspark.sql import *
from pyspark.sql.functions import *
# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Broadcast join").getOrCreate()
#sample data 
large_data = [(1,"Apple"),(2,"Banana"),(3,"Cherry"),(1,"Date"),(2,"Fig"),(3,"Grape")]
#Sample dF
small_data=[(1,"Fruit"),(2,"Berry"),(3,"Stone Fruit")]

large_df= spark.createDataFrame(large_data,["id","name"])
small_df= spark.createDataFrame(small_data,["id","type"])
#withoutbraodcast
join_df = large_df.join(small_df,small_df.id==large_df.id,"inner")
join_df.show()

from pyspark.sql import *
from pyspark.sql.functions import *
# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Broadcast join").getOrCreate()
#sample data 
large_data = [(1,"Apple"),(2,"Banana"),(3,"Cherry"),(1,"Date"),(2,"Fig"),(3,"Grape")]
#Sample dF
small_data=[(1,"Fruit"),(2,"Berry"),(3,"Stone Fruit")]

large_df= spark.createDataFrame(large_data,["id","name"])
small_df= spark.createDataFrame(small_data,["id","type"])
#with broadcast
join_df = large_df.join(broadcast(small_df),small_df.id==large_df.id,"inner")
join_df.show()

from pyspark.sql import *
from pyspark.sql.functions import *
# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Broadcast join").getOrCreate()
#sample data 
large_data = [(1,"Apple"),(2,"Banana"),(3,"Cherry"),(1,"Date"),(2,"Fig"),(3,"Grape")]
#Sample dF
small_data=[(1,"Fruit"),(2,"Berry"),(3,"Stone Fruit")]

large_df= spark.createDataFrame(large_data,["id","name"])
small_df= spark.createDataFrame(small_data,["id","type"])
partition_df = large_df.repartition(col("id"))
#with broadcast
join_df = partition_df.join(broadcast(small_df),small_df.id==partition_df.id,"inner")
join_df.show()


============================


1. convert the string of date to date column 
2. If date is less than 20210101 and status inActive then make balance as 0
3. If balance is negative then make 0 

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Broadcast join").getOrCreate()
# sample data 
data =[(101,"20210101",1000,"Active"),(102,"20200101",1000,"InActive"),(103,None,1000,"InActive"),(104,"20210201",-1000,"Active")]
schema=StructType([StructField("id",IntegerType(),True),StructField("date",StringType(),True),StructField("balance",IntegerType(),True),StructField("status",StringType(),True)])
df=spark.createDataFrame(data,schema)
df.printSchema()
df.show()
df_date = df.withColumn("date",to_date(col("date"),"yyyyMMdd"))
df_date.printSchema()
df_date.show()
date_null_df = df_date.filter(col("date").isNull())
date_null_df.show()
date_condition_df = df_date.filter(col("date")<"2021-01-01")
date_condition_df.show()
result_df = date_condition_df.union(date_null_df)
result_df.show()
df_date = df_date.withColumn("date",when(col("date").isNull(),lit("1900-01-01")).otherwise(col("date")))
result_df = df_date.withColumn("balance",when((col("status")=="InActive") & (col("date")<"2021-01-01"),0).when(col("balance")<0,0).otherwise(col("balance")))
result_df.show()

============================

Problem Statement: You have a DataFrame containing employee data with columns department and salary. Write a PySpark function to filter employees with a salary greater 60000 than amount, then count the number of employees in each department that meet this condition.


from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
data = [
    ("Engineering", 70000),
    ("Engineering", 80000),
    ("HR", 50000),
    ("HR", 55000),
    ("Marketing", 60000),
    ("Marketing", 65000),
    ("Sales", 40000)
]
columns = ["department", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.show()
salary_df = df.filter(col("salary")>60000)
salary_df.show()
group_df = salary_df.groupBy("department").agg(count("department").alias("count"))
group_df.show()

+-----------+-----+
| department|count|
+-----------+-----+
|Engineering|    2|
|  Marketing|    1|
+-----------+-----+


===================================

Convert string date column to date type 

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.master("local").appName("running revenue").getOrCreate()
data =[("2023-10-07", "15:30:00")]
columns =["date_str","time_str"]
df= spark.createDataFrame(data,columns)
df.show()
#convert to date 
formatted_df = df.withColumn("date_str",to_date(col("date_str"),"yyyy-MM-dd")).withColumn("time_str",to_timestamp(col("time_str")))
formatted_df.printSchema()
formatted_df.show()

+----------+-------------------+
|  date_str|           time_str|
+----------+-------------------+
|2023-10-07|2025-04-10 15:30:00|
+----------+-------------------+

=======================================================================

Problem: You need to perform calculations involving date and time values, such as finding the
difference between two dates or adding/subtracting days, months, or years.
Solution: Use Spark's date functions like datediff, date_add, date_sub, and add_months to perform
date arithmetic


===============================================

Aggregating by Date:
Problem: You want to group data by date and perform aggregations like sum or average on groups.
Solution: Use the groupBy function in Spark SQL along with date truncation functions like trunc to
aggregate data by date


from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.master("local").appName("Aggregating by date").getOrCreate()

date_data =[("2023-10-07", 10), ("2023-10-07", 15), ("2023-10-08", 20)]
date_columns=["date","value"]
df=spark.createDataFrame(date_data,date_columns)
df.printSchema()
df_date = df.withColumn("date",to_date(col("date")))
df_date.printSchema()
df_date.show()

result_df = df_date.groupBy(col("date")).agg(sum("value").alias("Total_value"))
result_df.show()

===================================================
Handling Null or Missing Dates:

Problem: Dealing with missing or null date values in your data.
Solution: Use Spark's coalesce function or the when function to handle null or missing dates by
providing default values or custom logic.

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.master("local").appName("Handling nulls with dates").getOrCreate()
date_data=[("2023-10-07", None), (None, "2023-10-08")]
date_columns=["date1","date2"]
df = spark.createDataFrame(date_data,date_columns)
df.printSchema()
df.show()
#convert to date 
date_df = df.withColumn("date1",to_date(col("date1"))).withColumn("date2",to_date(col("date2")))
date_df.printSchema()
date_df.show()
#Handling nulls in the data 
null_df = date_df.withColumn("date1",when(col("date1").isNull(),lit("1900-01-01")).otherwise(col("date1"))).withColumn("date2",when(col("date2").isNull(),lit("1900-01-01")).otherwise(col("date2")))
null_df.printSchema()
null_df.show()
null_df = null_df.withColumn("date1",to_date(col("date1"))).withColumn("date2",to_date(col("date2")))
null_df.printSchema()
null_df.show()



===============

Changing Date Formats:
Use the date_format function to change the format of date and timestamp columns
import org.apache.spark.sql.functions._
val df = Seq(("2023-10-07", "15:30:00")).toDF("date", "time")
val formattedDf = df.withColumn("formatted_date", date_format($"date", "yyyy/MM/dd"))
.withColumn("formatted_time", date_format($"time", "HH:mm:ss"))


from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.master("local").appName("changing date formats").getOrCreate()
date_format_data=[("2023-10-07", "15:30:00")]
date_format_columns = ["date","time"]
format_df = spark.createDataFrame(date_format_data,date_format_columns)
format_df.printSchema()
format_df.show()
#change the date formats
formatted_df = format_df.withColumn("date",date_format(col("date"),"yyyy/MM/dd")).withColumn("time",date_format(col("time"),"HH:mm:ss"))
formatted_df.printSchema()
formatted_df.show()

+----------+--------+
|      date|    time|
+----------+--------+
|2023/10/07|15:30:00|
+----------+--------+

===================================

Given a DataFrame with date and value columns, calculate the average value
for each month.

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.master("local").appName("calculate the average value").getOrCreate()

date_data=[("2023-10-07", 10), ("2023-10-07", 15), ("2023-11-08", 20)]
date_column = ["date","value"]
df = spark.createDataFrame(date_data,date_column)
df.printSchema()
df.show()
year_month_df = df.groupBy(date_format(col("date"),"yyyy-MM").alias("year_month")).agg(avg("value").alias("avg_value"))
year_month_df.show()

+----------+---------+
|year_month|avg_value|
+----------+---------+
|   2023-11|     20.0|
|   2023-10|     12.5|
+----------+---------+

==============================================

Given a DataFrame with timestamp and timezone columns, convert all
timestamps to a target time zone.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.master("local").appName("convert timestamps to a target time zone").getOrCreate()

timezome_data =[("2023-10-07 12:00:00", "UTC"), ("2023-10-07 12:00:00",
"America/New_York")]
timezone_columns=["timezone_str","timezone"]
timezone_df = spark.createDataFrame(timezome_data,timezone_columns)
timezone_df.printSchema()
timezone_df.show()
targetz_df = timezone_df.withColumn("converted_zone",from_utc_timestamp(col("timezone_str"),col("timezone")))
targetz_df.show()

+-------------------+----------------+
|       timezone_str|        timezone|
+-------------------+----------------+
|2023-10-07 12:00:00|             UTC|
|2023-10-07 12:00:00|America/New_York|
+-------------------+----------------+

+-------------------+----------------+-------------------+
|       timezone_str|        timezone|     converted_zone|
+-------------------+----------------+-------------------+
|2023-10-07 12:00:00|             UTC|2023-10-07 12:00:00|
|2023-10-07 12:00:00|America/New_York|2023-10-07 08:00:00|
+-------------------+----------------+-------------------+


===================
Given a DataFrame with date1 and date2 columns, handle missing date values
by filling them with default dates.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.master("local").appName("cHandling nulls/missing values with dates").getOrCreate()

data=[("2023-10-07", None), (None, "2023-10-08")]
columns=["date1","date2"]
df=spark.createDataFrame(data,columns)
df.show()
handling_null_df= df.withColumn("date1",when(col("date1").isNull(),lit("1900-01-01")).otherwise(col("date1"))).withColumn("date2",when(col("date2").isNull(),lit("1900-01-01")).otherwise(col("date2")))
handling_null_df.show()

+----------+----------+
|     date1|     date2|
+----------+----------+
|2023-10-07|      NULL|
|      NULL|2023-10-08|
+----------+----------+

+----------+----------+
|     date1|     date2|
+----------+----------+
|2023-10-07|1900-01-01|
|1900-01-01|2023-10-08|


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.master("local").appName("Extract day of the week").getOrCreate()

data = [("2023-10-07 12:00:00",),("2023-10-10 15:30:00",)]
columns=["timestamp_str"]
df = spark.createDataFrame(data,columns)
df.show()
df.printSchema()
df= df.withColumn("timestamp",to_timestamp(col("timestamp_str")))
df.show()
df.printSchema()
day_of_week_df = df.withColumn("day_of_week",date_format(col("timestamp"),"EEEE"))
day_of_week_df.show()

================================================================================================

 Given a DataFrame with a timestamp column, extract the day of the week for
each timestamp and display it as a new column.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 
spark = SparkSession.builder.master("local").appName("Extract day of the week").getOrCreate()

data = [("2023-10-07 12:00:00",),("2023-10-10 15:30:00",)]
columns=["timestamp_str"]
df = spark.createDataFrame(data,columns)
df.show()
df.printSchema()
df= df.withColumn("timestamp_str",to_timestamp(col("timestamp_str")))
df.show()
df.printSchema()
day_of_week_df = df.withColumn("day_of_week",date_format(col("timestamp_str"),"EEEE"))
day_of_week_df.show()

+-------------------+-----------+
|      timestamp_str|day_of_week|
+-------------------+-----------+
|2023-10-07 12:00:00|   Saturday|
|2023-10-10 15:30:00|    Tuesday|
+-------------------+-----------+

===================================



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff, when, initcap

# Create a SparkSession
spark = SparkSession.builder.appName("EmployeeStatus").getOrCreate()

# Sample data: (name, last_checkin_date)
data = [
    ("john doe", "2025-04-07"),
    ("jane smith", "2025-03-28"),
    ("alice johnson", "2025-04-04"),
    ("bob lee", "2025-03-31"),
]

# Create DataFrame
df = spark.createDataFrame(data, ["name", "last_checkin_date"])
df.printSchema()
df.show()
df = df.withColumn("last_checkin_date",to_date(col("last_checkin_date")))
df.printSchema()
employee_status_df = df.withColumn("status",when(datediff(current_date(),col("last_checkin_date"))<=7,lit("Active")).otherwise(lit("InActive")))
employee_status_df.show()

+-------------+-----------------+--------+
|         name|last_checkin_date|  status|
+-------------+-----------------+--------+
|     john doe|       2025-04-07|  Active|
|   jane smith|       2025-03-28|InActive|
|alice johnson|       2025-04-04|  Active|
|      bob lee|       2025-03-31|InActive|
+-------------+-----------------+--------+

=========================================================================================

find the 3rd highest salary

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 

# Create a SparkSession
spark = SparkSession.builder.appName("EmployeeStatus").getOrCreate()

# Sample data: (name, last_checkin_date)
data = [
    (1,"2024-02-01",35),
    (1,"2024-02-02",10),
    (1,"2024-02-03",10)
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "date","balance"])
df.printSchema()
df.show()
window_spec=Window.partitionBy(col("id")).orderBy(desc(col("balance")))
df_row_number=df.withColumn("3rdHistory",row_number().over(window_spec))
df_row_number.show()
df=df_row_number.filter(col("3rdHistory")==3)
df.show()


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import * 

# Create a SparkSession
spark = SparkSession.builder.appName("EmployeeStatus").getOrCreate()

# Sample data: (name, last_checkin_date)
data = [
    (1,"2024-02-01",35),
    (1,"2024-02-02",10),
    (1,"2024-02-03",10)
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "date","balance"])
df.printSchema()
df.show()
#window_spec=Window.partitionBy(col("id")).orderBy(desc(col("balance")))
window_spec=Window.orderBy(desc(col("balance")))
df_row_number=df.withColumn("3rdHistory",row_number().over(window_spec))
df_row_number.show()
df=df_row_number.filter(col("3rdHistory")==3)
df.show()


+---+----------+-------+----------+
| id|      date|balance|3rdHistory|
+---+----------+-------+----------+
|  1|2024-02-01|     35|         1|
|  1|2024-02-02|     10|         2|
|  1|2024-02-03|     10|         3|
+---+----------+-------+----------+

+---+----------+-------+----------+
| id|      date|balance|3rdHistory|
+---+----------+-------+----------+
|  1|2024-02-03|     10|         3|
+---+----------+-------+----------+


=================================

partition write 
===============

df.write.partitionBy("date").mode("overwrite").parquet("gs://bucket_path")

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("EmployeeStatus").getOrCreate()

# Sample data: (name, last_checkin_date)
data = [
    (1,"2024-02-01",35),
    (1,"2024-02-02",10),
    (1,"2024-02-03",10)
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "date","balance"])
df.printSchema()
df.show()
#window_spec=Window.partitionBy(col("id")).orderBy(desc(col("balance")))
window_spec=Window.orderBy(desc(col("balance")))
df_row_number=df.withColumn("3rdHistory",row_number().over(window_spec))
df_row_number.show()
df=df_row_number.filter(col("3rdHistory")==3)
df.show()
df.write.partitionBy("date").parquet("/content/sample_data/ThirdHighest")

===============================================================
Count nulls from each column 

from pyspark.sql import SparkSession
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Null Check Example") \
    .getOrCreate()
# Sample data
data = [
    ("Alice", 50, 1),
    (None, 60, 2),
    ("Bob", None, 3),
    ("Charlie", 70, None),
    (None, None, None)
]
# Creating DataFrame
columns = ["name", "value", "id"]
df = spark.createDataFrame(data, columns)
# Show DataFrame
df.show()
null_df=df.filter(col("value").isNull())
null_df.show()
#count how many nulls in each column
nulls_each_df = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
nulls_each_df.show()

+-------+-----+----+
|   name|value|  id|
+-------+-----+----+
|  Alice|   50|   1|
|   NULL|   60|   2|
|    Bob| NULL|   3|
|Charlie|   70|NULL|
|   NULL| NULL|NULL|
+-------+-----+----+

+----+-----+----+
|name|value|  id|
+----+-----+----+
| Bob| NULL|   3|
|NULL| NULL|NULL|
+----+-----+----+

+----+-----+---+
|name|value| id|
+----+-----+---+
|   2|    2|  2|
+----+-----+---+

==========================================================================
Problem Statement:
We need to find the genre of the person with the most Oscar wins. In case of a tie (multiple people with the same number of wins), we need to return the person who comes first alphabetically by their name.

Problem Breakdown
We will join two tables: nominee_information (contains details about the nominees) and oscar_nominees (contains the Oscar nominations and whether the nominee won). The primary goal is to count the total number of wins and identify the nominee with the highest count.

PySpark Solution:
Solution Outline:
Calculate the Number of Wins: We will first count the number of wins for each nominee by counting the rows where winner = 1 in the oscar_nominees table.
Sort and Join: After calculating the number of wins, we will sort by the number of wins in descending order and by name in ascending order to handle ties.
Return the Genre: We will join the results with the nominee_information table on the nominee’s name to retrieve the top_genre for the person with the most wins.

from pyspark.sql import SparkSession
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Null Check Example") \
    .getOrCreate()
# Sample data
# Sample data for nominee_information
nominee_data = [
    ('Jennifer Lawrence', 'P562566', 'Drama', '1990-08-15', 755),
    ('Jonah Hill', 'P418718', 'Comedy', '1983-12-20', 747),
    ('Anne Hathaway', 'P292630', 'Drama', '1982-11-12', 744),
    ('Jennifer Hudson', 'P454405', 'Drama', '1981-09-12', 742),
    ('Rinko Kikuchi', 'P475244', 'Drama', '1981-01-06', 739)
]
# Sample data for oscar_nominees
oscar_data = [
    (2008, 'actress in a leading role', 'Anne Hathaway', 'Rachel Getting Married', 0, 77),
    (2012, 'actress in a supporting role', 'Anne HathawayLes', 'Mis_rables', 1, 78),
    (2006, 'actress in a supporting role', 'Jennifer Hudson', 'Dreamgirls', 1, 711),
    (2010, 'actress in a leading role', 'Jennifer Lawrence', 'Winters Bone', 1, 717),
    (2012, 'actress in a leading role', 'Jennifer Lawrence', 'Silver Linings Playbook', 1, 718),
    (2011, 'actor in a supporting role', 'Jonah Hill', 'Moneyball', 0, 799),
    (2006, 'actress in a supporting role', 'Rinko Kikuchi', 'Babel', 0, 1253)
]

# Define schema for nominee_information
columns_nominee = ["name", "amg_person_id", "top_genre", "birthday", "id"]

# Define schema for oscar_nominees
columns_oscar = ["year", "category", "nominee", "movie", "winner", "id"]

df_nominations = spark.createDataFrame(nominee_data, columns_nominee)
df_oscar = spark.createDataFrame(oscar_data, columns_oscar)
df_nominations.show()
df_oscar.show()
#get the count of the winers 
df_winners = df_oscar.filter(col("winner")==1).groupBy(col("nominee")).agg(count("winner").alias("Total_winners"))
df_winners.show()
#get the details of the genre with oscar winners 
df_join_list = df_winners.join(df_nominations,df_nominations.name==df_winners.nominee,"inner")
df_join_list.show()
#order if the name is same based on the alphabetical order
sorted_df=df_join_list.orderBy(desc(col("Total_winners")),col("name"))
result_df = sorted_df.select(col("name"),col("top_genre")).first()
# Show the result
print(f"The genre of the person with the most Oscar wins is {result_df['top_genre']}.")

=========================================================================================================


Problem Statement
We have a table of employees that includes the following fields: id, first_name, last_name, age, sex, employee_title, department, salary, target, bonus, city, address, and manager_id. We need to find the top 3 distinct salaries for each department. The output should include:

The department name.
The top 3 distinct salaries for each department.
The results should be ordered alphabetically by department and then by the highest salary to the lowest salary.
Solution Explanation
We will solve this problem using the following steps in PySpark:

Read the Employee Data: We’ll start by creating a DataFrame that contains the employee data.
Select Distinct Salaries: Since we need distinct salaries, we will ensure we only select distinct salary values for each department.
Rank Salaries: We will use PySpark’s Window function to rank the salaries within each department.
Filter Top 3 Salaries: After ranking, we will filter out the top 3 salaries within each department.
Sort the Results: Finally, we will order the results alphabetically by department and then by the salary in descending order.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("TopSalariesByDepartment").getOrCreate()

# Sample data
data = [
    (1, 'Allen', 'Wang', 55, 'F', 'Manager', 'Management', 200000, 0, 300, 'California', '23St', 1),
    (13, 'Katty', 'Bond', 56, 'F', 'Manager', 'Management', 150000, 0, 300, 'Arizona', None, 1),
    (19, 'George', 'Joe', 50, 'M', 'Manager', 'Management', 100000, 0, 300, 'Florida', '26St', 1),
    (11, 'Richerd', 'Gear', 57, 'M', 'Manager', 'Management', 250000, 0, 300, 'Alabama', None, 1),
    (10, 'Jennifer', 'Dion', 34, 'F', 'Sales', 'Sales', 100000, 200, 150, 'Alabama', None, 13),
    (18, 'Laila', 'Mark', 26, 'F', 'Sales', 'Sales', 100000, 200, 150, 'Florida', '23St', 11),
    (20, 'Sarrah', 'Bicky', 31, 'F', 'Senior Sales', 'Sales', 200000, 200, 150, 'Florida', '53St', 19),
    (21, 'Suzan', 'Lee', 34, 'F', 'Sales', 'Sales', 130000, 200, 150, 'Florida', '56St', 19),
    (22, 'Mandy', 'John', 31, 'F', 'Sales', 'Sales', 130000, 200, 150, 'Florida', '45St', 19),
    (17, 'Mick', 'Berry', 44, 'M', 'Senior Sales', 'Sales', 220000, 200, 150, 'Florida', None, 11),
    (12, 'Shandler', 'Bing', 23, 'M', 'Auditor', 'Audit', 110000, 200, 150, 'Arizona', None, 11),
    (14, 'Jason', 'Tom', 23, 'M', 'Auditor', 'Audit', 100000, 200, 150, 'Arizona', None, 11),
    (16, 'Celine', 'Anston', 27, 'F', 'Auditor', 'Audit', 100000, 200, 150, 'Colorado', None, 11),
    (15, 'Michale', 'Jackson', 44, 'F', 'Auditor', 'Audit', 70000, 150, 150, 'Colorado', None, 11),
    (6, 'Molly', 'Sam', 28, 'F', 'Sales', 'Sales', 140000, 100, 150, 'Arizona', '24St', 13),
    (7, 'Nicky', 'Bat', 33, 'F', 'Sales', 'Sales', None, None, None, None, None, None)
]

# Define columns for the employees DataFrame
columns = ["id", "first_name", "last_name", "age", "sex", "employee_title", "department", "salary", "target", "bonus", "city", "address", "manager_id"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()
window_spec= Window.partitionBy(col("department")).orderBy(desc(col("salary")))
rank_df = df.withColumn("Rank",dense_rank().over(window_spec))
rank_df.show()
top3_distinct=rank_df.filter(col("Rank")<=3).select(col("department"),col("salary"),col("Rank")).distinct()
top3_distinct.show()



+----------+------+----+
|department|salary|rank|
+----------+------+----+
|Audit     |110000|1   |
|Audit     |100000|2   |
|Audit     |70000 |3   |
|Management|250000|1   |
|Management|200000|2   |
|Management|150000|3   |
|Sales     |220000|1   |
|Sales     |200000|2   |
|Sales     |140000|3   |
+----------+------+----+

+----------+------+----+
|department|salary|Rank|
+----------+------+----+
|     Audit|110000|   1|
|     Audit|100000|   2|
|     Audit| 70000|   3|
|Management|250000|   1|
|Management|200000|   2|
|Management|150000|   3|
|     Sales|220000|   1|
|     Sales|200000|   2|
|     Sales|140000|   3|
+----------+------+----+


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("TopSalariesByDepartment").getOrCreate()

# Sample data
data = [
    (1, 'Allen', 'Wang', 55, 'F', 'Manager', 'Management', 200000, 0, 300, 'California', '23St', 1),
    (13, 'Katty', 'Bond', 56, 'F', 'Manager', 'Management', 150000, 0, 300, 'Arizona', None, 1),
    (19, 'George', 'Joe', 50, 'M', 'Manager', 'Management', 100000, 0, 300, 'Florida', '26St', 1),
    (11, 'Richerd', 'Gear', 57, 'M', 'Manager', 'Management', 250000, 0, 300, 'Alabama', None, 1),
    (10, 'Jennifer', 'Dion', 34, 'F', 'Sales', 'Sales', 100000, 200, 150, 'Alabama', None, 13),
    (18, 'Laila', 'Mark', 26, 'F', 'Sales', 'Sales', 100000, 200, 150, 'Florida', '23St', 11),
    (20, 'Sarrah', 'Bicky', 31, 'F', 'Senior Sales', 'Sales', 200000, 200, 150, 'Florida', '53St', 19),
    (21, 'Suzan', 'Lee', 34, 'F', 'Sales', 'Sales', 130000, 200, 150, 'Florida', '56St', 19),
    (22, 'Mandy', 'John', 31, 'F', 'Sales', 'Sales', 130000, 200, 150, 'Florida', '45St', 19),
    (17, 'Mick', 'Berry', 44, 'M', 'Senior Sales', 'Sales', 220000, 200, 150, 'Florida', None, 11),
    (12, 'Shandler', 'Bing', 23, 'M', 'Auditor', 'Audit', 110000, 200, 150, 'Arizona', None, 11),
    (14, 'Jason', 'Tom', 23, 'M', 'Auditor', 'Audit', 100000, 200, 150, 'Arizona', None, 11),
    (16, 'Celine', 'Anston', 27, 'F', 'Auditor', 'Audit', 100000, 200, 150, 'Colorado', None, 11),
    (15, 'Michale', 'Jackson', 44, 'F', 'Auditor', 'Audit', 70000, 150, 150, 'Colorado', None, 11),
    (6, 'Molly', 'Sam', 28, 'F', 'Sales', 'Sales', 140000, 100, 150, 'Arizona', '24St', 13),
    (7, 'Nicky', 'Bat', 33, 'F', 'Sales', 'Sales', None, None, None, None, None, None)
]

# Define columns for the employees DataFrame
columns = ["id", "first_name", "last_name", "age", "sex", "employee_title", "department", "salary", "target", "bonus", "city", "address", "manager_id"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Step 1: Select distinct department and salary
distinct_salaries_df = df.select("department", "salary").distinct()
distinct_salaries_df.show()
# Step 2: Create a window spec for ranking salaries within each department
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

# Step 3: Rank salaries within each department
ranked_df = distinct_salaries_df.withColumn("rank", rank().over(window_spec))

# Step 4: Filter the top 3 salaries for each department
top_salaries_df = ranked_df.filter(col("rank") <= 3)

# Step 5: Sort results by department name and salary in descending order
result_df = top_salaries_df.orderBy("department", col("salary").desc())

# Show the final results
result_df.show(truncate=False)


==========================================
Question 2: Sales Performance by Agent
Given a DataFrame of sales agents with their total sales amounts, calculate the performance status
based on sales thresholds: “Excellent” if sales are above 50,000, “Good” if between 25,000 and
50,000, and “Needs Improvement” if below 25,000. Capitalize each agent&#39;s name, and show total
sales aggregated by performance status.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("TopSalariesByDepartment").getOrCreate()
sales = [
("karthik", 60000),
("neha", 48000),
("priya", 30000),
("mohan", 24000),
("ajay", 52000),
("vijay", 45000),
("veer", 70000),
("aatish", 23000),
("animesh", 15000),
("nishad", 8000),
("varun", 29000),
("aadil", 32000)
]
columns=["name","total_sales"]
total_sales = spark.createDataFrame(sales,columns)
total_sales.show()
performance_df = total_sales.withColumn("name",upper(col("name"))).withColumn("performance",when(col("total_sales")>50000,"Excellent").when((col("total_sales") >25000) & (col("total_sales")<50000),"Good").otherwise("Needs improvement"))
performance_df.show()
result_df = performance_df.groupBy("performance").agg(sum("total_sales").alias("Total_sales"))
result_df.show()



+-------+-----------+-----------------+
|   name|total_sales|      performance|
+-------+-----------+-----------------+
|KARTHIK|      60000|        Excellent|
|   NEHA|      48000|             Good|
|  PRIYA|      30000|             Good|
|  MOHAN|      24000|Needs improvement|
|   AJAY|      52000|        Excellent|
|  VIJAY|      45000|             Good|
|   VEER|      70000|        Excellent|
| AATISH|      23000|Needs improvement|
|ANIMESH|      15000|Needs improvement|
| NISHAD|       8000|Needs improvement|
|  VARUN|      29000|             Good|
|  AADIL|      32000|             Good|
+-------+-----------+-----------------+

+-----------------+-----------+
|      performance|Total_sales|
+-----------------+-----------+
|        Excellent|     182000|
|             Good|     184000|
|Needs improvement|      70000|
+-----------------+-----------+


===========================================================


Given a DataFrame with project allocation data for multiple employees, determine each employee&#39;s
workload level based on their hours worked in a month across various projects. Categorize
employees as “Overloaded” if they work more than 200 hours, “Balanced” if between 100-200
hours, and “Underutilized” if below 100 hours. Capitalize each employee’s name, and show the
aggregated workload status count by category.


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("workload Analysis").getOrCreate()
workload = [
("karthik", "ProjectA", 120),
("karthik", "ProjectB", 100),
("neha", "ProjectC", 80),
("neha", "ProjectD", 30),
("priya", "ProjectE", 110),
("mohan", "ProjectF", 40),
("ajay", "ProjectG", 70),
("vijay", "ProjectH", 150),
("veer", "ProjectI", 190),
("aatish", "ProjectJ", 60),
("animesh", "ProjectK", 95),
("nishad", "ProjectL", 210),
("varun", "ProjectM", 50),
("aadil", "ProjectN", 90)
]
columns = ["name","project","hours"]

df = spark.createDataFrame(workload,columns)
df.show()
df.printSchema()
workload_df = df.withColumn("name",upper(col("name"))).withColumn("workload_analysis",when(col("hours")>200,"Overloaded").
                                                                  when((col("hours")>100)&(col("hours")<200),"Balanced").otherwise("Underloaded"))
workload_df.show()
result_df = workload_df.groupBy("workload_analysis").agg(sum("hours").alias("Total_hours"))
result_df.show()
result_df.write.partitionBy("workload_analysis").parquet("/content/sample_data/workload_analysis")

+-------+--------+-----+-----------------+
|   name| project|hours|workload_analysis|
+-------+--------+-----+-----------------+
|KARTHIK|ProjectA|  120|         Balanced|
|KARTHIK|ProjectB|  100|      Underloaded|
|   NEHA|ProjectC|   80|      Underloaded|
|   NEHA|ProjectD|   30|      Underloaded|
|  PRIYA|ProjectE|  110|         Balanced|
|  MOHAN|ProjectF|   40|      Underloaded|
|   AJAY|ProjectG|   70|      Underloaded|
|  VIJAY|ProjectH|  150|         Balanced|
|   VEER|ProjectI|  190|         Balanced|
| AATISH|ProjectJ|   60|      Underloaded|
|ANIMESH|ProjectK|   95|      Underloaded|
| NISHAD|ProjectL|  210|       Overloaded|
|  VARUN|ProjectM|   50|      Underloaded|
|  AADIL|ProjectN|   90|      Underloaded|
+-------+--------+-----+-----------------+

+-----------------+-----------+
|workload_analysis|Total_hours|
+-----------------+-----------+
|      Underloaded|        615|
|         Balanced|        570|
|       Overloaded|        210|
+-----------------+-----------+

============================================================
Determine whether an employee has "Excessive Overtime" if their weekly hours exceed 60,
"Standard Overtime" if between 45-60 hours, and "No Overtime" if below 45 hours. Capitalize each
name and group by overtime status.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("workload Analysis").getOrCreate()
employees = [
("karthik", 62),
("neha", 50),
("priya", 30),
("mohan", 65),
("ajay", 40),
("vijay", 47),
("veer", 55),
("aatish", 30),
("animesh", 75),
("nishad", 60)
]
columns=["name","hours_worked"]
df = spark.createDataFrame(employees,columns)
df.show()
overtime_df = df.withColumn("name",upper(col("name"))).withColumn("overtime",when(col("hours_worked")>50,"Excessive Overtime").when((col("hours_worked") > 45) & (col("hours_worked")< 60),"Standard Overtime").otherwise("No Overtime"))
overtime_df.show()
result_df = overtime_df.groupBy("overtime").agg(sum("hours_worked").alias("Total_hours"))
result_df.orderBy(col("Total_hours")).show()



+-------+------------+------------------+
|   name|hours_worked|          overtime|
+-------+------------+------------------+
|KARTHIK|          62|Excessive Overtime|
|   NEHA|          50| Standard Overtime|
|  PRIYA|          30|       No Overtime|
|  MOHAN|          65|Excessive Overtime|
|   AJAY|          40|       No Overtime|
|  VIJAY|          47| Standard Overtime|
|   VEER|          55|Excessive Overtime|
| AATISH|          30|       No Overtime|
|ANIMESH|          75|Excessive Overtime|
| NISHAD|          60|Excessive Overtime|
+-------+------------+------------------+

+------------------+-----------+
|          overtime|Total_hours|
+------------------+-----------+
| Standard Overtime|         97|
|       No Overtime|        100|
|Excessive Overtime|        317|
+------------------+-----------+

=======================================
Customer Age Grouping
Group customers as "Youth" if under 25, "Adult" if between 25-45, and "Senior" if over 45. Capitalize
names and show total customers in each group.



from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Customer Age grouping").getOrCreate()
customers = [
("karthik", 22),
("neha", 28),
("priya", 40),
("mohan", 55),
("ajay", 32),
("vijay", 18),
("veer", 47),
("aatish", 38),
("animesh", 60),
("nishad", 25)
]
columns = ["name","age"]
df = spark.createDataFrame(customers,columns)
df.show()
age_grouping_df = df.withColumn("name",upper(col("name"))).withColumn("cust_age_group",when(col("age")<25,"Youth").
                                                                      when((col("age")>=25) & (col("age")<45),"Adult").otherwise("Senior"))
age_grouping_df.show()
result_df = age_grouping_df.groupBy("cust_age_group").agg(count("name").alias("Total_customers_under_each_group"))
result_df.show()

+-------+---+--------------+
|   name|age|cust_age_group|
+-------+---+--------------+
|KARTHIK| 22|         Youth|
|   NEHA| 28|         Adult|
|  PRIYA| 40|         Adult|
|  MOHAN| 55|        Senior|
|   AJAY| 32|         Adult|
|  VIJAY| 18|         Youth|
|   VEER| 47|        Senior|
| AATISH| 38|         Adult|
|ANIMESH| 60|        Senior|
| NISHAD| 25|         Adult|
+-------+---+--------------+

+--------------+--------------------------------+
|cust_age_group|Total_customers_under_each_group|
+--------------+--------------------------------+
|        Senior|                               3|
|         Youth|                               2|
|         Adult|                               5|
+--------------+--------------------------------+

================================================================================================================
Vehicle Mileage Analysis
Classify each vehicle’s mileage as "High Efficiency" if mileage is above 25 MPG, "Moderate Efficiency"
if between 15-25 MPG, and "Low Efficiency" if below 15 MPG.




from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Vechicle mileage").getOrCreate()
vehicles = [
("CarA", 30),

("CarB", 22),
("CarC", 18),
("CarD", 15),
("CarE", 10),
("CarF", 28),
("CarG", 12),
("CarH", 35),
("CarI", 25),
("CarJ", 16)
]
columns =["vehicle_name","mileage"]
df = spark.createDataFrame(vehicles,columns)
df.show()
mileage_df = df.withColumn("Mileage",when(col("mileage")>25,"High Efficiency").when((col("mileage")>=15) & (col("mileage")<25),"Moderate Efficiency").otherwise("Low Efficiency"))
mileage_df.orderBy(col("Mileage")).show() 

+------------+-------+
|vehicle_name|mileage|
+------------+-------+
|        CarA|     30|
|        CarB|     22|
|        CarC|     18|
|        CarD|     15|
|        CarE|     10|
|        CarF|     28|
|        CarG|     12|
|        CarH|     35|
|        CarI|     25|
|        CarJ|     16|
+------------+-------+

+------------+-------------------+
|vehicle_name|            Mileage|
+------------+-------------------+
|        CarA|    High Efficiency|
|        CarF|    High Efficiency|
|        CarH|    High Efficiency|
|        CarE|     Low Efficiency|
|        CarG|     Low Efficiency|
|        CarI|     Low Efficiency|
|        CarB|Moderate Efficiency|
|        CarJ|Moderate Efficiency|
|        CarC|Moderate Efficiency|
|        CarD|Moderate Efficiency|
+------------+-------------------+

=====================================================
Student Grade Classification
Classify students based on their scores as "Excellent" if score is 90 or above, "Good" if between 75-
89, and "Needs Improvement" if below 75. Count students in each category.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Grade_classification").getOrCreate()
students = [
("karthik", 95),
("neha", 82),
("priya", 74),
("mohan", 91),
("ajay", 67),
("vijay", 80),
("veer", 85),
("aatish", 72),
("animesh", 90),
("nishad", 60)
]
columns=["name","score"]
df = spark.createDataFrame(students,columns)
df.show()
grade_df = df.withColumn("Grade_classification",when(col("score")>=90,"Excellent").
                         when((col("score")>=75) & (col("score")<=89),"Good").otherwise("Needs Improvement"))
grade_df.show()
results_grade_df= grade_df.groupBy("Grade_classification").agg(count(col("name")).alias("Total_students"))
results_grade_df.show()

+-------+-----+--------------------+
|   name|score|Grade_classification|
+-------+-----+--------------------+
|karthik|   95|           Excellent|
|   neha|   82|                Good|
|  priya|   74|   Needs Improvement|
|  mohan|   91|           Excellent|
|   ajay|   67|   Needs Improvement|
|  vijay|   80|                Good|
|   veer|   85|                Good|
| aatish|   72|   Needs Improvement|
|animesh|   90|           Excellent|
| nishad|   60|   Needs Improvement|
+-------+-----+--------------------+

+--------------------+--------------+
|Grade_classification|Total_students|
+--------------------+--------------+
|           Excellent|             3|
|   Needs Improvement|             4|
|                Good|             3|
+--------------------+--------------+
========================================================================
Product Inventory Check
Classify inventory stock levels as ";Overstocked"; if stock exceeds 100, ";Normal"; if between 50-100,
and ";Low Stock"; if below 50. Aggregate total stock in each category.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Grade_classification").getOrCreate()
inventory = [
("ProductA", 120),
("ProductB", 95),
("ProductC", 45),
("ProductD", 200),
("ProductE", 75),
("ProductF", 30),
("ProductG", 85),
("ProductH", 100),
("ProductI", 60),
("ProductJ", 20)
]
columns_data = ["product_name","stock_quantity"]
df = spark.createDataFrame(inventory,columns_data)
df.show()
stock_df = df.withColumn("stock_classification",when(col("stock_quantity")>100,"Over_stock").
                         when((col("stock_quantity")>=50) & (col("stock_quantity")<=100),"Normal").otherwise("Low_stock"))
stock_df.show()
result_df = stock_df.groupBy("stock_classification").agg(sum("stock_quantity").alias("Total_stock"))
result_df.show()

+------------+--------------+--------------------+
|product_name|stock_quantity|stock_classification|
+------------+--------------+--------------------+
|    ProductA|           120|          Over_stock|
|    ProductB|            95|              Normal|
|    ProductC|            45|           Low_stock|
|    ProductD|           200|          Over_stock|
|    ProductE|            75|              Normal|
|    ProductF|            30|           Low_stock|
|    ProductG|            85|              Normal|
|    ProductH|           100|              Normal|
|    ProductI|            60|              Normal|
|    ProductJ|            20|           Low_stock|
+------------+--------------+--------------------+

+--------------------+-----------+
|stock_classification|Total_stock|
+--------------------+-----------+
|          Over_stock|        320|
|           Low_stock|         95|
|              Normal|        415|
+--------------------+-----------+

=============================================================
Employee Bonus Calculation Based on Performance and Department
Classify employees for a bonus eligibility program. Employees in "Sales" and "Marketing" with
performance scores above 80 get a 20% bonus, while others with scores above 70 get 15%. All other
employees receive no bonus. Group by department and calculate total bonus allocation.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Calculation Based on Performance").getOrCreate()

employees = [
("karthik", "Sales", 85),
("neha", "Marketing", 78),
("priya", "IT", 90),
("mohan", "Finance", 65),
("ajay", "Sales", 55),
("vijay", "Marketing", 82),
("veer", "HR", 72),
("aatish", "Sales", 88),
("animesh", "Finance", 95),
("nishad", "IT", 60)
]

columns_data =["name","department","performance_score"]
df = spark.createDataFrame(employees,columns_data)
df.show()
sales_marketing_df = df.filter(col("department").isin(["Sales","Marketing"])).withColumn("Bonus_calculation",when(col("performance_score")>80,20.0).
                               when((col("performance_score")>70) & (col("performance_score")<80),15.0).otherwise(0.0))

sales_marketing_df.show()
result_df = sales_marketing_df.groupBy("department").agg(first(col("name")),first(col("Bonus_calculation")),round(sum("Bonus_calculation"),2).alias("Total_Bonus_calculation"))
result_df.show()

+-------+----------+-----------------+-----------------+
|   name|department|performance_score|Bonus_calculation|
+-------+----------+-----------------+-----------------+
|karthik|     Sales|               85|             20.0|
|   neha| Marketing|               78|             15.0|
|   ajay|     Sales|               55|              0.0|
|  vijay| Marketing|               82|             20.0|
| aatish|     Sales|               88|             20.0|
+-------+----------+-----------------+-----------------+

+----------+-----------+------------------------+-----------------------+
|department|first(name)|first(Bonus_calculation)|Total_Bonus_calculation|
+----------+-----------+------------------------+-----------------------+
| Marketing|       neha|                    15.0|                   35.0|
|     Sales|    karthik|                    20.0|                   40.0|
+----------+-----------+------------------------+-----------------------+


===================

Product Return Analysis with Multi-Level Classification
For each product, classify return reasons as "High Return Rate" if return count exceeds 100 and
satisfaction score below 50, "Moderate Return Rate" if return count is between 50-100 with a score
between 50-70, and "Low Return Rate" otherwise. Group by category to count product return rates.

Shows the product name, return rate classification, and total number of
products in each return rate category for each category.


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Product Return Analysis").getOrCreate()

products = [
("Laptop", "Electronics", 120, 45),
("Smartphone", "Electronics", 80, 60),
("Tablet", "Electronics", 50, 72),
("Headphones", "Accessories", 110, 47),
("Shoes", "Clothing", 90, 55),
("Jacket", "Clothing", 30, 80),
("TV", "Electronics", 150, 40),
("Watch", "Accessories", 60, 65),
("Pants", "Clothing", 25, 75),
("Camera", "Electronics", 95, 58)
]

columns_data_type=["product","category","return_count","satisfaction_score"]

df = spark.createDataFrame(products,columns_data_type)
df.show()
return_analysis_df = df.withColumn("Return_Analysis",when((col("return_count")>100) &(col("satisfaction_score")<50),"High Return Rate").
                                   when((col("return_count").between(50,100)) & (col("satisfaction_score").between(50,70)),"Moderate Return Rate").otherwise("Low Return Rate"))
return_analysis_df.select(col("product"),col("category"),col("return_count"),col("satisfaction_score"),col("Return_Analysis")).show()
result_df = return_analysis_df.groupBy(col("category"),col("Return_Analysis")).agg(count(col("product")))
result_df.show()

+----------+-----------+------------+------------------+--------------------+
|   product|   category|return_count|satisfaction_score|     Return_Analysis|
+----------+-----------+------------+------------------+--------------------+
|    Laptop|Electronics|         120|                45|    High Return Rate|
|Smartphone|Electronics|          80|                60|Moderate Return Rate|
|    Tablet|Electronics|          50|                72|     Low Return Rate|
|Headphones|Accessories|         110|                47|    High Return Rate|
|     Shoes|   Clothing|          90|                55|Moderate Return Rate|
|    Jacket|   Clothing|          30|                80|     Low Return Rate|
|        TV|Electronics|         150|                40|    High Return Rate|
|     Watch|Accessories|          60|                65|Moderate Return Rate|
|     Pants|   Clothing|          25|                75|     Low Return Rate|
|    Camera|Electronics|          95|                58|Moderate Return Rate|
+----------+-----------+------------+------------------+--------------------+

+-----------+--------------------+--------------+
|   category|     Return_Analysis|count(product)|
+-----------+--------------------+--------------+
|Electronics|    High Return Rate|             2|
|Electronics|     Low Return Rate|             1|
|Accessories|    High Return Rate|             1|
|Electronics|Moderate Return Rate|             2|
|   Clothing|Moderate Return Rate|             1|
|   Clothing|     Low Return Rate|             2|
|Accessories|Moderate Return Rate|             1|
+-----------+--------------------+--------------+


=====================================================================
Classify customer's spending as "High Spender" if spending exceeds $1000 with "Premium"
membership, "Average Spender" if spending between $500-$1000 and membership is "Standard",
and "Low Spender" otherwise. Group by membership and calculate average spending.

Output: Displays customer's names, spending category, and average spending by
membership type.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Classify customers").getOrCreate()
customers = [
("karthik", "Premium", 1050, 32),
("neha", "Standard", 800, 28),
("priya", "Premium", 1200, 40),
("mohan", "Basic", 300, 35),
("ajay", "Standard", 700, 25),
("vijay", "Premium", 500, 45),
("veer", "Basic", 450, 33),
("aatish", "Standard", 600, 29),
("animesh", "Premium", 1500, 60),
("nishad", "Basic", 200, 21)
]

columns_data = ["name", "membership", "spending", "age"]
df = spark.createDataFrame(customers,columns_data)
df.show()
membership_df=df.withColumn("Spender_analysis",when((col("membership")=="Premium") & (col("spending")>1000),"High Spender").
                            when((col("membership")=="Standard") & (col("spending").between(500,1000)),"Average Spender").otherwise("Low Spender"))
membership_df.show()
membership_df.select(col("name"),col("membership"),col("spending"),col("Spender_analysis")).show()
group_df = membership_df.groupBy(col("membership")).agg(avg("spending").alias("avg_spending"))
group_df.show()


+-------+----------+--------+---+----------------+
|   name|membership|spending|age|Spender_analysis|
+-------+----------+--------+---+----------------+
|karthik|   Premium|    1050| 32|    High Spender|
|   neha|  Standard|     800| 28| Average Spender|
|  priya|   Premium|    1200| 40|    High Spender|
|  mohan|     Basic|     300| 35|     Low Spender|
|   ajay|  Standard|     700| 25| Average Spender|
|  vijay|   Premium|     500| 45|     Low Spender|
|   veer|     Basic|     450| 33|     Low Spender|
| aatish|  Standard|     600| 29| Average Spender|
|animesh|   Premium|    1500| 60|    High Spender|
| nishad|     Basic|     200| 21|     Low Spender|
+-------+----------+--------+---+----------------+

+-------+----------+--------+----------------+
|   name|membership|spending|Spender_analysis|
+-------+----------+--------+----------------+
|karthik|   Premium|    1050|    High Spender|
|   neha|  Standard|     800| Average Spender|
|  priya|   Premium|    1200|    High Spender|
|  mohan|     Basic|     300|     Low Spender|
|   ajay|  Standard|     700| Average Spender|
|  vijay|   Premium|     500|     Low Spender|
|   veer|     Basic|     450|     Low Spender|
| aatish|  Standard|     600| Average Spender|
|animesh|   Premium|    1500|    High Spender|
| nishad|     Basic|     200|     Low Spender|
+-------+----------+--------+----------------+

+----------+-----------------+
|membership|     avg_spending|
+----------+-----------------+
|   Premium|           1062.5|
|     Basic|316.6666666666667|
|  Standard|            700.0|
+----------+-----------------+

=============================================

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("BucketingJoinExample").getOrCreate()

# Sample DataFrame 1
data1 = [
    ("Alice", 1, "2023-01-01"),
    ("Bob", 2, "2023-01-02"),
    ("Cathy", 3, "2023-01-03"),
]
columns1 = ["Name", "ID", "Date"]

df1 = spark.createDataFrame(data1, columns1)

# Sample DataFrame 2
data2 = [
    (1, "Product A"),
    (2, "Product B"),
    (3, "Product C"),
]
columns2 = ["ID", "Product"]

df2 = spark.createDataFrame(data2, columns2)

# Write DataFrames with Bucketing
# Both DataFrames are bucketed by the ID column with the same number of buckets

df1.write.bucketBy(2, "ID").sortBy("Date").mode("overwrite").saveAsTable("bucketed_table3")
df2.write.bucketBy(2, "ID").sortBy("Product").mode("overwrite").saveAsTable("bucketed_table3")

# Perform a Join on the Bucketed Tables
result_df = spark.sql("""
    SELECT a.Name, a.Date, b.Product 
    FROM bucketed_table1 a
    JOIN bucketed_table2 b
    ON a.ID = b.ID
""")

result_df.show()

=======================================

PArtition and bucket together


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# Initialize Spark Session
spark = SparkSession.builder.appName("PartitioningBucketingExample").getOrCreate()

# Sample DataFrame
data = [
    ("Alice", "USA", 1, "2023-01-01"),
    ("Bob", "UK", 2, "2023-01-02"),
    ("Cathy", "USA", 3, "2023-01-03"),
    ("David", "UK", 4, "2023-01-04"),
    ("Eve", "USA", 5, "2023-01-05"),
]
columns = ["Name", "Country", "UserID", "Date"]

df = spark.createDataFrame(data, columns)
df.write.partitionBy("Country").bucketBy(2,"UserId").sortBy("Date").mode("overwrite").saveAsTable("partitioned_bucketed_table")
result_df = spark.sql(""" select * from partitioned_bucketed_table
where Country='USA' and UserId=3 """)
result_df.show()



===========================


read the data from a csv file with custom schema and transform the data to find the managers with more than 5 direct employees and write the final data with partition into parquet file 




from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark
spark = SparkSession.builder.appName("ManagerEmployeeCount").getOrCreate()

# Sample data
data = [
    (1, "Alice", 3),
    (2, "Bob", 3),
    (3, "Charlie", None),
    (4, "David", 3),
    (5, "Eve", 3),
    (6, "Frank", 3),
    (7, "Grace", 3),
]

columns = ["emp_id", "name", "manager_id"]

df = spark.createDataFrame(data,columns)
# find the manager with 5 direct employess
manager_df = df.groupBy(col("manager_id")).agg(count("*").alias("num_reports")).filter(col("num_reports")>5)
manager_df.show()
join_df = manager_df.alias("m").join(df.alias("e"),col("e.emp_id") == col("m.manager_id"),"inner")
join_df.show()
result_df = join_df.select(col("e.emp_id").alias("manager_id"),col("e.name").alias("manager_name"),col("num_reports"))
result_df.show()
result_df.write.partitionBy("manager_id").parquet("/content/sample_data/manager_employee_count")


+----------+-----------+
|manager_id|num_reports|
+----------+-----------+
|         3|          6|
+----------+-----------+

+----------+-----------+------+-------+----------+
|manager_id|num_reports|emp_id|   name|manager_id|
+----------+-----------+------+-------+----------+
|         3|          6|     3|Charlie|      NULL|
+----------+-----------+------+-------+----------+

+----------+------------+-----------+
|manager_id|manager_name|num_reports|
+----------+------------+-----------+
|         3|     Charlie|          6|
+----------+------------+-----------+


====================================================

Write pyspark coding to get the maximum mark for students marks for subject wise

from pyspark.sql import SparkSession
from pyspark.sql.functions import max

# Initialize SparkSession
spark = SparkSession.builder.appName("MaxMarksPerSubject").getOrCreate()

# Sample data
data = [
    (1, "Math", 85),
    (2, "Math", 92),
    (3, "English", 78),
    (4, "English", 88),
    (5, "Science", 91),
]

columns = ["student_id", "subject", "marks"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
max_df = df.groupBy(col("subject")).agg(max("marks").alias("max_amrks"))
max_df.show()


+-------+---------+
|subject|max_amrks|
+-------+---------+
|   Math|       92|
|Science|       91|
|English|       88|
+-------+---------+

how to replace null column with na

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize SparkSession
spark = SparkSession.builder.appName("MaxMarksPerSubject").getOrCreate()

# Sample data
data = [
    (1, "Math", 85),
    (2, "Math", 92),
    (3, "English", 78),
    (4, "English", 88),
    (5, "Science", 91),
    (6, "Science", None),
]

columns = ["student_id", "subject", "marks"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
null_df = df.withColumn("marks",when(col("marks").isNull(),lit(0)).otherwise(col("marks")))
null_df.show()
max_df = null_df.groupBy(col("subject")).agg(max("marks").alias("max_amrks"))
max_df.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize SparkSession
spark = SparkSession.builder.appName("MaxMarksPerSubject").getOrCreate()

# Sample data
data = [
    (1, "Math", 85),
    (2, "Math", 92),
    (3, "English", 78),
    (4, "English", 88),
    (5, "Science", 91),
    (6, "Science", None),
]

columns = ["student_id", "subject", "marks"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
nulls_df = df.na.fill(0)
nulls_df.show()
null_df = df.withColumn("marks",when(col("marks").isNull(),lit(0)).otherwise(col("marks")))
null_df.show()
max_df = null_df.groupBy(col("subject")).agg(max("marks").alias("max_amrks"))
max_df.show()

+----------+-------+-----+
|student_id|subject|marks|
+----------+-------+-----+
|         1|   Math|   85|
|         2|   Math|   92|
|         3|English|   78|
|         4|English|   88|
|         5|Science|   91|
|         6|Science|    0|
+----------+-------+-----+

+----------+-------+-----+
|student_id|subject|marks|
+----------+-------+-----+
|         1|   Math|   85|
|         2|   Math|   92|
|         3|English|   78|
|         4|English|   88|
|         5|Science|   91|
|         6|Science|    0|
+----------+-------+-----+

+-------+---------+
|subject|max_amrks|
+-------+---------+
|   Math|       92|
|English|       88|
|Science|       91|
+-------+---------+


====================================

find the date difference b/w two dates

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("difference b/w two dates").getOrCreate()
date_data =[("2024-01-01","2024-02-01")]
columns_data = ["date1","date2"]
df = spark.createDataFrame(date_data,columns_data)
df.printSchema()
df.show()
#convert to date 
df= df.withColumn("date1",to_date(col("date1"))).withColumn("date2",to_date(col("date2")))
df.printSchema()
#date diff
results_date_df = df.withColumn("date_diff",datediff(col("date2"),col("date1")))
results_date_df.show()


+----------+----------+---------+
|     date1|     date2|date_diff|
+----------+----------+---------+
|2024-01-01|2024-02-01|       31|
+----------+----------+---------+


==================================================

given dataset of cricket matches with columns 
"matchId","team1","team2","winner","date".
write pyspark code to find the team with most wins in year 2024

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("difference b/w two dates").getOrCreate()
# Sample DataFrame - replace with your actual data loading
data = [
    (1, "India", "Australia", "India", "2024-01-15"),
    (2, "England", "India", "India", "2024-02-20"),
    (3, "Australia", "England", "Australia", "2024-03-10"),
    (4, "India", "England", "England", "2023-12-30"),
]

columns = ["matchId", "team1", "team2", "winner", "date"]
df = spark.createDataFrame(data, columns)
df.show()
#convert date column to date 
df= df.withColumn("date",to_date(col("date")))
df.printSchema()
#filter records for 2024 
df_2024 = df.filter(year(col("date"))=="2024")
df_2024.show()
#get the most wins teams 
most_wins_df = df_2024.groupBy(col("winner")).agg(count("*").alias("most_wins"))
most_wins_df.orderBy(desc("most_wins")).show()

+-------+---------+---------+---------+----------+
|matchId|    team1|    team2|   winner|      date|
+-------+---------+---------+---------+----------+
|      1|    India|Australia|    India|2024-01-15|
|      2|  England|    India|    India|2024-02-20|
|      3|Australia|  England|Australia|2024-03-10|
|      4|    India|  England|  England|2023-12-30|
+-------+---------+---------+---------+----------+


+-------+---------+---------+---------+----------+
|matchId|    team1|    team2|   winner|      date|
+-------+---------+---------+---------+----------+
|      1|    India|Australia|    India|2024-01-15|
|      2|  England|    India|    India|2024-02-20|
|      3|Australia|  England|Australia|2024-03-10|
+-------+---------+---------+---------+----------+

+---------+---------+
|   winner|most_wins|
+---------+---------+
|    India|        2|
|Australia|        1|
+---------+---------+

==================================
Dataset contains "TransactionId","customerId","TransactionAmount","TransactionDate"
write pyspark to determine the total transaction amount per customer for the month january 2025 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, sum as _sum, to_date

# Start Spark session
spark = SparkSession.builder.appName("TotalTransactionPerCustomerJan2025").getOrCreate()

# Sample data
data = [
    (1, "C001", 100.0, "2025-01-05"),
    (2, "C002", 150.0, "2025-01-15"),
    (3, "C001", 200.0, "2025-01-20"),
    (4, "C003", 120.0, "2024-12-31"),
    (5, "C002", 80.0, "2025-02-01"),
]

columns = ["TransactionId", "customerId", "TransactionAmount", "TransactionDate"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, sum as _sum, to_date

# Start Spark session
spark = SparkSession.builder.appName("TotalTransactionPerCustomerJan2025").getOrCreate()

# Sample data
data = [
    (1, "C001", 100.0, "2025-01-05"),
    (2, "C002", 150.0, "2025-01-15"),
    (3, "C001", 200.0, "2025-01-20"),
    (4, "C003", 120.0, "2024-12-31"),
    (5, "C002", 80.0, "2025-02-01"),
]

columns = ["TransactionId", "customerId", "TransactionAmount", "TransactionDate"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()
df = df.withColumn("TransactionDate",to_date(col("TransactionDate")))
df.printSchema()
df.show()
jan_month_df = df.filter((year(col("TransactionDate"))==2025) & (month(col("TransactionDate"))==1))
jan_month_df.show()
result_sum_df = jan_month_df.groupBy(col("customerId")).agg(sum(col("TransactionAmount")).alias("Total_Transaction_Amount"))
result_sum_df.show()

+-------------+----------+-----------------+---------------+
|TransactionId|customerId|TransactionAmount|TransactionDate|
+-------------+----------+-----------------+---------------+
|            1|      C001|            100.0|     2025-01-05|
|            2|      C002|            150.0|     2025-01-15|
|            3|      C001|            200.0|     2025-01-20|
|            4|      C003|            120.0|     2024-12-31|
|            5|      C002|             80.0|     2025-02-01|
+-------------+----------+-----------------+---------------+

root
 |-- TransactionId: long (nullable = true)
 |-- customerId: string (nullable = true)
 |-- TransactionAmount: double (nullable = true)
 |-- TransactionDate: date (nullable = true)

+-------------+----------+-----------------+---------------+
|TransactionId|customerId|TransactionAmount|TransactionDate|
+-------------+----------+-----------------+---------------+
|            1|      C001|            100.0|     2025-01-05|
|            2|      C002|            150.0|     2025-01-15|
|            3|      C001|            200.0|     2025-01-20|
|            4|      C003|            120.0|     2024-12-31|
|            5|      C002|             80.0|     2025-02-01|
+-------------+----------+-----------------+---------------+

+-------------+----------+-----------------+---------------+
|TransactionId|customerId|TransactionAmount|TransactionDate|
+-------------+----------+-----------------+---------------+
|            1|      C001|            100.0|     2025-01-05|
|            2|      C002|            150.0|     2025-01-15|
|            3|      C001|            200.0|     2025-01-20|
+-------------+----------+-----------------+---------------+

+----------+------------------------+
|customerId|Total_Transaction_Amount|
+----------+------------------------+
|      C001|                   300.0|
|      C002|                   150.0|


=====================

You said:
dataset with inidan cities with columns: "city","state","population","area"
write pyspark code to calculate population density for each city and find the top 5 densely populated cities

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Start Spark session
spark = SparkSession.builder.appName("CityPopulationDensity").getOrCreate()

# Sample data
data = [
    ("Mumbai", "Maharashtra", 20411000, 603),
    ("Delhi", "Delhi", 16787941, 1484),
    ("Kolkata", "West Bengal", 14617882, 185),
    ("Chennai", "Tamil Nadu", 7090000, 426),
    ("Bengaluru", "Karnataka", 8443675, 709),
    ("Hyderabad", "Telangana", 6809970, 650)
]

columns = ["city", "state", "population", "area"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
#calculate population density 
population_density = df.withColumn("population_density",(col("population")/col("area")))
population_density.show()
results_df = population_density.orderBy(col("population_density").desc()).limit(5)
results_df.select(col("city"),col("state"),col("population_density")).show()

+---------+-----------+----------+----+------------------+
|     city|      state|population|area|population_density|
+---------+-----------+----------+----+------------------+
|   Mumbai|Maharashtra|  20411000| 603|33849.087893864016|
|    Delhi|      Delhi|  16787941|1484| 11312.62870619946|
|  Kolkata|West Bengal|  14617882| 185| 79015.57837837838|
|  Chennai| Tamil Nadu|   7090000| 426| 16643.19248826291|
|Bengaluru|  Karnataka|   8443675| 709|11909.273624823696|
|Hyderabad|  Telangana|   6809970| 650|10476.876923076923|
+---------+-----------+----------+----+------------------+

+---------+-----------+------------------+
|     city|      state|population_density|
+---------+-----------+------------------+
|  Kolkata|West Bengal| 79015.57837837838|
|   Mumbai|Maharashtra|33849.087893864016|
|  Chennai| Tamil Nadu| 16643.19248826291|
|Bengaluru|  Karnataka|11909.273624823696|
|    Delhi|      Delhi| 11312.62870619946|
+---------+-----------+------------------+

=======================================================

Karthik-- problem statement

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("age Group classification").getOrCreate()
data = [
 ("Aarav", 92, 18, "Mumbai", "Math"),
 ("Sanya", 85, 19, "Delhi", "Physics"),
 ("Rohan", 75, 17, "Bangalore", "Chemistry"),
 ("Meera", 60, 20, "Hyderabad", "Biology"),
 ("Ishaan", 45, 18, "Kolkata", "English")
]
age_columns=["name","marks","age","city","subject"]
df = spark.createDataFrame(data,age_columns)
df.show()
#Age group classification 
result_df = df.select("*",when(col("age")<18,lit("minor")).when(col("age").between(18,21),lit("Young adult")).otherwise(lit("Adult")).alias("age_group"))
result_df.show()
#sparkSql 
df.createOrReplaceTempView("age_group")
spark.sql(""" select name,marks,age,city,subject, case when age <18 then 'Minor' when age between 18 and 21 then 'Young adult'
else 'Adult' end as age_group from age_group""").show()

+------+-----+---+---------+---------+-----------+
|  name|marks|age|     city|  subject|  age_group|
+------+-----+---+---------+---------+-----------+
| Aarav|   92| 18|   Mumbai|     Math|Young adult|
| Sanya|   85| 19|    Delhi|  Physics|Young adult|
| Rohan|   75| 17|Bangalore|Chemistry|      minor|
| Meera|   60| 20|Hyderabad|  Biology|Young adult|
|Ishaan|   45| 18|  Kolkata|  English|Young adult|
+------+-----+---+---------+---------+-----------+

+------+-----+---+---------+---------+-----------+
|  name|marks|age|     city|  subject|  age_group|
+------+-----+---+---------+---------+-----------+
| Aarav|   92| 18|   Mumbai|     Math|Young adult|
| Sanya|   85| 19|    Delhi|  Physics|Young adult|
| Rohan|   75| 17|Bangalore|Chemistry|      Minor|
| Meera|   60| 20|Hyderabad|  Biology|Young adult|
|Ishaan|   45| 18|  Kolkata|  English|Young adult|
+------+-----+---+---------+---------+-----------+

================================

Problem 3: Metro City Flag
Add a column metro_city:
• If city is Mumbai, Delhi, or Bangalore → "Yes"
• Otherwise → "No"

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("Metro City Flag").getOrCreate()
data = [
 ("Aarav", 92, 18, "Mumbai", "Math"),
 ("Sanya", 85, 19, "Delhi", "Physics"),
 ("Rohan", 75, 17, "Bangalore", "Chemistry"),
 ("Meera", 60, 20, "Hyderabad", "Biology"),
 ("Ishaan", 45, 18, "Kolkata", "English")
]
age_columns=["name","marks","age","city","subject"]
df = spark.createDataFrame(data,age_columns)
df.show()
metro_df= df.select("*",when(col("city").isin("Mumbai","Delhi","Bangalore"),lit("Metro City")).otherwise("No").alias("Metro-Flag"))
metro_df.show()
#Spark sql 
df.createOrReplaceTempView("metro_city")
spark.sql(""" 
select name,marks,age,city,subject, 
case when city in ("Mumbai","Delhi","Bangalore") then 'Metro city' else 'No' end as Metro_flag from metro_city""").show()

+------+-----+---+---------+---------+----------+
|  name|marks|age|     city|  subject|Metro-Flag|
+------+-----+---+---------+---------+----------+
| Aarav|   92| 18|   Mumbai|     Math|Metro City|
| Sanya|   85| 19|    Delhi|  Physics|Metro City|
| Rohan|   75| 17|Bangalore|Chemistry|Metro City|
| Meera|   60| 20|Hyderabad|  Biology|        No|
|Ishaan|   45| 18|  Kolkata|  English|        No|
+------+-----+---+---------+---------+----------+

+------+-----+---+---------+---------+----------+
|  name|marks|age|     city|  subject|Metro_flag|
+------+-----+---+---------+---------+----------+
| Aarav|   92| 18|   Mumbai|     Math|Metro city|
| Sanya|   85| 19|    Delhi|  Physics|Metro city|
| Rohan|   75| 17|Bangalore|Chemistry|Metro city|
| Meera|   60| 20|Hyderabad|  Biology|        No|
|Ishaan|   45| 18|  Kolkata|  English|        No|
+------+-----+---+---------+---------+----------+

Problem 4: Subject Difficulty
Add a column difficulty_level:
• If subject is Math or Physics → "High"
• If subject is Chemistry or Biology → "Medium"
• If subject is English → "Low"

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("Subject Difficulty").getOrCreate()
data = [
 ("Aarav", 92, 18, "Mumbai", "Math"),
 ("Sanya", 85, 19, "Delhi", "Physics"),
 ("Rohan", 75, 17, "Bangalore", "Chemistry"),
 ("Meera", 60, 20, "Hyderabad", "Biology"),
 ("Ishaan", 45, 18, "Kolkata", "English")
]
age_columns=["name","marks","age","city","subject"]
df = spark.createDataFrame(data,age_columns)
df.show()
subject_difficulty_df = df.withColumn("subject_difficulty",when((col("subject") == "Math") | (col("subject")=="Physics"),lit("High")).when((col("subject")=="Chemistry")| (col("subject")=="Biology"),lit("Medium")).otherwise(lit("Low")).alias("subject_difficulty"))
subject_difficulty_df.show()
df.createOrReplaceTempView("subject_difficulty")
spark.sql(""" """)

+------+-----+---+---------+---------+------------------+
|  name|marks|age|     city|  subject|subject_difficulty|
+------+-----+---+---------+---------+------------------+
| Aarav|   92| 18|   Mumbai|     Math|              High|
| Sanya|   85| 19|    Delhi|  Physics|              High|
| Rohan|   75| 17|Bangalore|Chemistry|            Medium|
| Meera|   60| 20|Hyderabad|  Biology|            Medium|
|Ishaan|   45| 18|  Kolkata|  English|               Low|
+------+-----+---+---------+---------+------------------+

======================================================

Count the number of people in each department

Find average score by department

Count the number of entries for each year

Find maximum score per city

Minimum score per department

Count how many people are from each location

Total score for each department

Count of people per department and year

Average score per location

Find departments where more than 2 people are present

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("Group by Problems").getOrCreate()
data = [
  ("Aarav Sharma", "Engineering", 95, "Mumbai", 2023),
  ("Sanya Mehta", "Leadership", 88, "Delhi", 2023),
  ("Rohan Iyer", "Science", 92, "Pune", 2022),
  ("Meera Nair", "Espionage", 85, "Chennai", 2023),
  ("Yash Verma", "Combat", 75, "Bhopal", 2022),
  ("Kiran Joshi", "Science", 78, "Mumbai", 2023),
  ("Anaya Reddy", "Magic", 89, "Hyderabad", 2022),
  ("Kabir Singh", "Leadership", 82, "Lucknow", 2023),
  ("Devansh Kapoor", "Magic", 94, "Mumbai", 2022),
  ("Nikhil Desai", "Engineering", 72, "Ahmedabad", 2022),
  ("Ishita Roy", "Science", 98, "Kolkata", 2023),
  ("Arjun Bhatia", "Combat", 90, "Jaipur", 2023)
]

columns=["name", "department", "score", "location", "year"]
df=spark.createDataFrame(data,columns)
df.show()
#Count the number of people in each department
depart_df = df.groupBy(col("department")).agg(count(col("name")).alias("count_of_people"))
depart_df.show()
#Find average score by department
avg_df=df.groupBy(col("department")).agg(avg(col("score")).alias("avg_score"))
avg_df.show()
#Count the number of entries for each year
year_df = df.groupBy(col("year")).agg(count(col("name")).alias("count_of_entries"))
year_df.show()
#Find maximum score per city
max_df = df.groupBy(col("location")).agg(max(col("score")).alias("maximum"))
max_df.show()
#Minimum score per department
min_df = df.groupBy(col("department")).agg(min(col("score")).alias("minimum"))
min_df.show()
#Count how many people are from each location
people_df = df.groupBy(col("location")).agg(count(col("name")).alias("people_count"))
people_df.show()
#Total score for each department
Total_score_df = df.groupBy(col("department")).agg(sum(col("score")).alias("Total score"))
Total_score_df.show()
#Count of people per department and year
Total_people_df = df.groupBy(col("department"),col("year")).agg(count(col("name")).alias("Total people"))
Total_people_df.show()
#Average score per location
avg_score_df = df.groupBy(col("location")).agg(avg(col("score")).alias("Average score per location"))
avg_score_df.show()
#Find departments where more than 2 people are present
filter_df = df.groupBy(col("department")).agg(count(col("name")).alias("cnt"))
filter_df.filter(col("cnt")>2).show()


+--------------+-----------+-----+---------+----+
|          name| department|score| location|year|
+--------------+-----------+-----+---------+----+
|  Aarav Sharma|Engineering|   95|   Mumbai|2023|
|   Sanya Mehta| Leadership|   88|    Delhi|2023|
|    Rohan Iyer|    Science|   92|     Pune|2022|
|    Meera Nair|  Espionage|   85|  Chennai|2023|
|    Yash Verma|     Combat|   75|   Bhopal|2022|
|   Kiran Joshi|    Science|   78|   Mumbai|2023|
|   Anaya Reddy|      Magic|   89|Hyderabad|2022|
|   Kabir Singh| Leadership|   82|  Lucknow|2023|
|Devansh Kapoor|      Magic|   94|   Mumbai|2022|
|  Nikhil Desai|Engineering|   72|Ahmedabad|2022|
|    Ishita Roy|    Science|   98|  Kolkata|2023|
|  Arjun Bhatia|     Combat|   90|   Jaipur|2023|
+--------------+-----------+-----+---------+----+

+-----------+---------------+
| department|count_of_people|
+-----------+---------------+
|    Science|              3|
|Engineering|              2|
| Leadership|              2|
|  Espionage|              1|
|     Combat|              2|
|      Magic|              2|
+-----------+---------------+

+-----------+-----------------+
| department|        avg_score|
+-----------+-----------------+
|    Science|89.33333333333333|
|Engineering|             83.5|
| Leadership|             85.0|
|  Espionage|             85.0|
|     Combat|             82.5|
|      Magic|             91.5|
+-----------+-----------------+

+----+----------------+
|year|count_of_entries|
+----+----------------+
|2022|               5|
|2023|               7|
+----+----------------+

+---------+-------+
| location|maximum|
+---------+-------+
|  Chennai|     85|
|   Mumbai|     95|
|     Pune|     92|
|    Delhi|     88|
|   Bhopal|     75|
|  Lucknow|     82|
|Ahmedabad|     72|
|  Kolkata|     98|
|Hyderabad|     89|
|   Jaipur|     90|
+---------+-------+

+-----------+-------+
| department|minimum|
+-----------+-------+
|    Science|     78|
|Engineering|     72|
| Leadership|     82|
|  Espionage|     85|
|     Combat|     75|
|      Magic|     89|
+-----------+-------+

+---------+------------+
| location|people_count|
+---------+------------+
|  Chennai|           1|
|   Mumbai|           3|
|     Pune|           1|
|    Delhi|           1|
|   Bhopal|           1|
|  Lucknow|           1|
|Ahmedabad|           1|
|  Kolkata|           1|
|Hyderabad|           1|
|   Jaipur|           1|
+---------+------------+

+-----------+-----------+
| department|Total score|
+-----------+-----------+
|    Science|        268|
|Engineering|        167|
| Leadership|        170|
|  Espionage|         85|
|     Combat|        165|
|      Magic|        183|
+-----------+-----------+

+-----------+----+------------+
| department|year|Total people|
+-----------+----+------------+
|    Science|2023|           2|
|  Espionage|2023|           1|
|    Science|2022|           1|
|     Combat|2022|           1|
| Leadership|2023|           2|
|Engineering|2023|           1|
|     Combat|2023|           1|
|      Magic|2022|           2|
|Engineering|2022|           1|
+-----------+----+------------+

+---------+--------------------------+
| location|Average score per location|
+---------+--------------------------+
|  Chennai|                      85.0|
|   Mumbai|                      89.0|
|     Pune|                      92.0|
|    Delhi|                      88.0|
|   Bhopal|                      75.0|
|  Lucknow|                      82.0|
|Ahmedabad|                      72.0|
|  Kolkata|                      98.0|
|Hyderabad|                      89.0|
|   Jaipur|                      90.0|
+---------+--------------------------+

+----------+---+
|department|cnt|
+----------+---+
|   Science|  3|
+----------+---+
==========================================

Windows Pyspark 
---------------
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("Windows Analysis function").getOrCreate()
df = spark.read.format("csv").option("header",True).option("inferSchema",True).load("/content/sample_sales_pyspark.csv")
df.show()
# window function 
window_spec = Window.partitionBy(col("store_code"),col("product_code")).orderBy(col("sales_date"))
#cummulative sum of the sales_qty
sum_df = df.withColumn("total_sales",sum(col("sales_qty")).over(window_spec))
results_df = sum_df.filter(col("store_code")=="B1").select("store_code", "product_code", "sales_date", "sales_qty", "total_sales")
results_df.show()
+----------+------------+----------+---------+-----------+
|store_code|product_code|sales_date|sales_qty|total_sales|
+----------+------------+----------+---------+-----------+
|        B1|       89912|2021-05-01|       14|         14|
|        B1|       89912|2021-05-02|       19|         33|
|        B1|       89912|2021-05-03|       15|         48|
|        B1|       89912|2021-05-04|       21|         69|
|        B1|       89912|2021-05-05|        4|         73|
|        B1|       89912|2021-05-06|        5|         78|
|        B1|       89912|2021-05-07|       10|         88|
|        B1|       89912|2021-05-08|       18|        106|
|        B1|       89912|2021-05-09|        5|        111|
|        B1|       89912|2021-05-10|        2|        113|
|        B1|       89912|2021-05-11|       15|        128|
|        B1|       89912|2021-05-12|       21|        149|
|        B1|       89912|2021-05-13|        7|        156|
|        B1|       89912|2021-05-14|       17|        173|
|        B1|       89912|2021-05-15|       16|        189|
|        B1|       89915|2021-05-01|       20|         20|
|        B1|       89915|2021-05-02|        0|         20|
|        B1|       89915|2021-05-03|       10|         30|
|        B1|       89915|2021-05-04|       13|         43|
|        B1|       89915|2021-05-05|       21|         64|
+----------+------------+----------+---------+-----------+

=========
max()
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("Windows Analysis function - max").getOrCreate()
df = spark.read.format("csv").option("header",True).option("inferSchema",True).load("/content/sample_sales_pyspark.csv")
df.show()
# window function 
window_spec = Window.partitionBy(col("store_code"),col("product_code")).orderBy(col("sales_date"))
#cummulative sum of the sales_qty
sum_df = df.withColumn("max_price",max(col("price")).over(window_spec))
results_df = sum_df.filter(col("store_code")=="B1").select("store_code", "product_code", "sales_date", "price", "max_price")
results_df.show()

+----------+------------+----------+-----+---------+
|store_code|product_code|sales_date|price|max_price|
+----------+------------+----------+-----+---------+
|        B1|       89912|2021-05-01| 1261|     1261|
|        B1|       89912|2021-05-02| 1278|     1278|
|        B1|       89912|2021-05-03| 1287|     1287|
|        B1|       89912|2021-05-04| 1347|     1347|
|        B1|       89912|2021-05-05| 1351|     1351|
|        B1|       89912|2021-05-06| 1355|     1355|
|        B1|       89912|2021-05-07| 1242|     1355|
|        B1|       89912|2021-05-08| 1250|     1355|
|        B1|       89912|2021-05-09| 1311|     1355|
|        B1|       89912|2021-05-10| 1319|     1355|
|        B1|       89912|2021-05-11| 1305|     1355|
|        B1|       89912|2021-05-12| 1342|     1355|
|        B1|       89912|2021-05-13| 1324|     1355|
|        B1|       89912|2021-05-14| 1328|     1355|
|        B1|       89912|2021-05-15| 1270|     1355|
|        B1|       89915|2021-05-01| 1352|     1352|
|        B1|       89915|2021-05-02| 1361|     1361|
|        B1|       89915|2021-05-03| 1231|     1361|
|        B1|       89915|2021-05-04| 1241|     1361|
|        B1|       89915|2021-05-05| 1329|     1361|
+----------+------------+----------+-----+---------+

============

Lead and lag 

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("Windows Analysis func
tion - lead and lag").getOrCreate()
df = spark.read.format("csv").option("header",True).option("inferSchema",True).load("/content/sample_sales_pyspark.csv")
df.show()
# window function 
window_spec = Window.partitionBy(col("store_code"),col("product_code")).orderBy(col("sales_date"))
#lead of the previous day sales
df = df.withColumn("prev_day_sales_lead",lead(col("sales_qty"),-1).over(window_spec)).withColumn("prev_day_sales_lag",lag(col("sales_qty"),1).over(window_spec))

results_df = df.filter((col("store_code")=="A1") & (col("product_code")=="95955")).select("sales_date", "sales_qty", "prev_day_sales_lag", "prev_day_sales_lead")
results_df.show()

+----------+---------+------------------+-------------------+
|sales_date|sales_qty|prev_day_sales_lag|prev_day_sales_lead|
+----------+---------+------------------+-------------------+
|2021-05-01|       13|              NULL|               NULL|
|2021-05-02|        3|                13|                 13|
|2021-05-03|       22|                 3|                  3|
|2021-05-04|       17|                22|                 22|
|2021-05-05|       20|                17|                 17|
|2021-05-06|       14|                20|                 20|
|2021-05-07|       10|                14|                 14|
|2021-05-08|       10|                10|                 10|
|2021-05-09|       15|                10|                 10|
|2021-05-10|       15|                15|                 15|
|2021-05-11|        8|                15|                 15|
|2021-05-12|        9|                 8|                  8|
|2021-05-13|       13|                 9|                  9|
|2021-05-14|        6|                13|                 13|
|2021-05-15|       21|                 6|                  6|
+----------+---------+------------------+-------------------+

====================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("last_3_day_avg").getOrCreate()
df = spark.read.format("csv").option("header",True).option("inferSchema",True).load("/content/sample_sales_pyspark.csv")
df.show()
# window function 
window_spec = Window.partitionBy(col("store_code"),col("product_code")).orderBy(col("sales_date")).rowsBetween(-3,-1)
#lead of the previous day sales
df = df.withColumn("last_3_day_avg",mean(col("sales_qty")).over(window_spec))

results_df = df.filter((col("store_code")=="A1") & (col("product_code")=="95955")).select("sales_date", "sales_qty", "last_3_day_avg")
results_df.show()

+----------+---------+------------------+
|sales_date|sales_qty|    last_3_day_avg|
+----------+---------+------------------+
|2021-05-01|       13|              NULL|
|2021-05-02|        3|              13.0|
|2021-05-03|       22|               8.0|
|2021-05-04|       17|12.666666666666666|
|2021-05-05|       20|              14.0|
|2021-05-06|       14|19.666666666666668|
|2021-05-07|       10|              17.0|
|2021-05-08|       10|14.666666666666666|
|2021-05-09|       15|11.333333333333334|
|2021-05-10|       15|11.666666666666666|
|2021-05-11|        8|13.333333333333334|
|2021-05-12|        9|12.666666666666666|
|2021-05-13|       13|10.666666666666666|
|2021-05-14|        6|              10.0|
|2021-05-15|       21| 9.333333333333334|
+----------+---------+------------------+

=============
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("unboundedPreceding").getOrCreate()
df = spark.read.format("csv").option("header",True).option("inferSchema",True).load("/content/sample_sales_pyspark.csv")
df.show()
# window function 
window_spec = Window.partitionBy(col("store_code"),col("product_code")).orderBy(col("sales_date")).rowsBetween(Window.unboundedPreceding,Window.currentRow)
#lead of the previous day sales
df = df.withColumn("cumulative_mean",mean(col("sales_qty")).over(window_spec))

results_df = df.filter((col("store_code")=="A1") & (col("product_code")=="95955")).select("sales_date", "sales_qty", "cumulative_mean")
results_df.show()

+----------+---------+------------------+
|sales_date|sales_qty|   cumulative_mean|
+----------+---------+------------------+
|2021-05-01|       13|              13.0|
|2021-05-02|        3|               8.0|
|2021-05-03|       22|12.666666666666666|
|2021-05-04|       17|             13.75|
|2021-05-05|       20|              15.0|
|2021-05-06|       14|14.833333333333334|
|2021-05-07|       10|14.142857142857142|
|2021-05-08|       10|            13.625|
|2021-05-09|       15|13.777777777777779|
|2021-05-10|       15|              13.9|
|2021-05-11|        8|13.363636363636363|
|2021-05-12|        9|              13.0|
|2021-05-13|       13|              13.0|
|2021-05-14|        6|              12.5|
|2021-05-15|       21|13.066666666666666|
+----------+---------+------------------+

===============
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("unboundedPreceding").getOrCreate()
df = spark.read.format("csv").option("header",True).option("inferSchema",True).load("/content/sample_sales_pyspark.csv")
df.show()
# window function 
window_spec = Window.partitionBy(col("store_code"),col("product_code")).orderBy(col("sales_date")).rowsBetween(Window.unboundedPreceding,-1)
#lead of the previous day sales
df = df.withColumn("cumulative_mean",mean(col("sales_qty")).over(window_spec))

results_df = df.filter((col("store_code")=="A1") & (col("product_code")=="95955")).select("sales_date", "sales_qty", "cumulative_mean")
results_df.show()

+----------+---------+------------------+
|sales_date|sales_qty|   cumulative_mean|
+----------+---------+------------------+
|2021-05-01|       13|              NULL|
|2021-05-02|        3|              13.0|
|2021-05-03|       22|               8.0|
|2021-05-04|       17|12.666666666666666|
|2021-05-05|       20|             13.75|
|2021-05-06|       14|              15.0|
|2021-05-07|       10|14.833333333333334|
|2021-05-08|       10|14.142857142857142|
|2021-05-09|       15|            13.625|
|2021-05-10|       15|13.777777777777779|
|2021-05-11|        8|              13.9|
|2021-05-12|        9|13.363636363636363|
|2021-05-13|       13|              13.0|
|2021-05-14|        6|              13.0|
|2021-05-15|       21|              12.5|
+----------+---------+------------------+


===============================

We have a table of hotels with various attributes, including the hotel name, address, total reviews, and an average score based on the reviews. We need to find the top 10 hotels with the highest average scores. The output should include:

The hotel name.
The average score of the hotel.
The records should be sorted by average score in descending order.


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("TopHotelsByRating").getOrCreate()

# Sample data
data = [('123 Ocean Ave, Miami, FL', 3, '2024-11-10', 4.2, 'Ocean View', 'American', 'Room small, but clean.', 5, 150, 'Great location and friendly staff!', 8, 30, 4.5, 'beachfront, family-friendly', '5 days', 25.7617, -80.1918),    ('456 Mountain Rd, Boulder, CO', 2, '2024-11-12', 3.9, 'Mountain Lodge', 'Canadian', 'wifi slow.', 3, 120, 'nice rooms.', 10, 20, 4.0, 'scenic, nature', '3 days', 40.015, -105.2705),    ('789 Downtown St, New York, NY', 5, '2024-11-15', 4.7, 'Central Park Hotel', 'British', 'Noisy, sleep.', 7, 200, 'Perfect location near Central Park.', 12, 50, 4.7, 'luxury, city-center', '1 day', 40.7831, -73.9712),    ('101 Lakeside Blvd, Austin, TX', 1, '2024-11-08', 4.0, 'Lakeside Inn', 'Mexican', 'food avg.', 4, 80, 'Nice, friendly service.', 6, 15, 3.8, 'relaxing, family', '10 days', 30.2672, -97.7431),    ('202 River Ave, Nashville, TN', 4, '2024-11-13', 4.5, 'Riverside', 'German', 'Limited parking', 2, 175, 'Great rooms.', 9, 25, 4.2, 'riverfront, peaceful', '2 days', 36.1627, -86.7816)]

# Define columns for the hotel DataFrame
columns = ["hotel_address", "additional_number_of_scoring", "review_date", "average_score", "hotel_name",            "reviewer_nationality", "negative_review", "review_total_negative_word_counts", "total_number_of_reviews",           "positive_review", "review_total_positive_word_counts", "total_number_of_reviews_reviewer_has_given",            "reviewer_score", "tags", "days_since_review", "lat", "lng"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()
sort_df = df.orderBy(col("average_score").desc())
final_df = sort_df.select(col("hotel_name"),col("average_score")).limit(10)
final_df.show()

+------------------+-------------+
|        hotel_name|average_score|
+------------------+-------------+
|Central Park Hotel|          4.7|
|         Riverside|          4.5|
|        Ocean View|          4.2|
|      Lakeside Inn|          4.0|
|    Mountain Lodge|          3.9|
+------------------+-------------+


==========================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("Rank employees").getOrCreate()
data=[("John","HR",60000),("Jane","HR",65000),("Jake","HR",60000),("Alice","IT",80000),("Bob","IT",90000),("Charlie","IT",85000)]
columns=["name","department","salary"]
df = spark.createDataFrame(data,columns)
df.show()
#window
window_spec = Window.partitionBy(col("department")).orderBy(col("salary"))
lag_df = df.withColumn("prev_salary",lag(col("salary")).over(window_spec))
#rst_df= lag_df.withColumn("growth_status",when(col("prev_salary").isNull(),"No growth").when(col("salary").gt(col("prev_salary")),"High Growth").when(col("salary").lt(col("prev_salary")),"Low growth").otherwise("No growth"))
rst_df= lag_df.withColumn("growth_status",when(col("prev_salary").isNull(),"No growth").when(col("salary") > (col("prev_salary")),"High Growth").when(col("salary") < (col("prev_salary")),"Low growth").otherwise("No growth"))
rst_df.show()


+-------+----------+------+
|   name|department|salary|
+-------+----------+------+
|   John|        HR| 60000|
|   Jane|        HR| 65000|
|   Jake|        HR| 60000|
|  Alice|        IT| 80000|
|    Bob|        IT| 90000|
|Charlie|        IT| 85000|
+-------+----------+------+

+-------+----------+------+-----------+-------------+
|   name|department|salary|prev_salary|growth_status|
+-------+----------+------+-----------+-------------+
|   John|        HR| 60000|       NULL|    No growth|
|   Jake|        HR| 60000|      60000|    No growth|
|   Jane|        HR| 65000|      60000|  High Growth|
|  Alice|        IT| 80000|       NULL|    No growth|
|Charlie|        IT| 85000|      80000|  High Growth|
|    Bob|        IT| 90000|      85000|  High Growth|
+-------+----------+------+-----------+-------------+
====================================================
Group By
--------
1. per
2. each 


Group data by certifications and count how many professionals have each certification.
5. Calculate the total number of professionals in each specialization.
6. Determine the maximum experience_years for each current_bank.
7. Find the minimum experience level in each education_level.


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName("group By - - karthik").getOrCreate()
schemaBanking = StructType([
 StructField("full_name", StringType(), True),
 StructField("experience_years", FloatType(), True),
 StructField("specialization", StringType(), True),
 StructField("location", StringType(), True),
 StructField("education_level", StringType(), True),
 StructField("current_bank", StringType(), True),
 StructField("certifications", StringType(), True)
])
dataBanking = [("Rahul Khanna", 10.0, "Investment Banking, Risk Management", "Mumbai", "MBA", "ICICI Bank", "CFA"),
 ("Meera Desai", 7.5, "Retail Banking, Wealth Management", "Delhi", "B.Com", "HDFC Bank",
"CFP"),
 ("Arjun Malhotra", 5.2, "Corporate Finance, Treasury", "Bengaluru", "M.Tech", "Axis Bank",
"FRM"),
 ("Sneha Kapoor", 8.0, "Credit Risk, Loan Processing", "Hyderabad", "BBA", "SBI", "CFRM"),
 ("Vikram Joshi", 3.5, "Payment Systems, Digital Banking", "Pune", "B.Tech", "Kotak Mahindra",
"None"),
 ("Tanisha Rao", 6.7, "Private Banking, Portfolio Management", "Chennai", "M.Com", "Standard Chartered", "CAIA"),
 ("Rohan Agarwal", 4.1, "Forex Trading, Financial Markets", "Indore", "B.Sc", "Citi Bank", "Series7"),
 ("Neha Saxena", 9.3, "Fintech Solutions, Blockchain Banking", "Noida", "MBA", "IDFC First Bank", "CBP"),
 ("Harish Iyer", 2.8, "Retail Loans, Mortgage Processing", "Jaipur", "BCA", "Bank of Baroda",
"None"),
 ("Kavya Mehta", 7.0, "Economic Analysis, Financial Modeling", "Kolkata", "Ph.D", "RBI", "CMA")
]
df = spark.createDataFrame(dataBanking, schemaBanking)
df.show()
#Group by employees and count the no.of employees 
employee_df = df.groupBy(col("location")).agg(count(col("full_name")).alias("Total_employee"))
employee_df.show()
#education level
cert_df = df.groupBy(col("certifications")).agg(count(col("full_name")).alias("Each certifcation"))
cert_df.show()
# Calculate the total number of professionals in each specialization.
spec_df = df.groupBy(col("specialization")).agg(count(col("full_name")).alias("Total_professionals"))
spec_df.show()
#Determine the maximum experience_years for each current_bank.
curr_df = df.groupBy(col("current_bank")).agg(max(col("experience_years")).alias("Max_year"))
curr_df.show()
#Find the minimum experience level in each education_level.
edu_df = df.groupBy(col("education_level")).agg(min(col("experience_years")).alias("Min_exp_year"))
edu_df.show()

---------+--------------+
| location|Total_employee|
+---------+--------------+
|   Mumbai|             1|
|     Pune|             1|
|    Delhi|             1|
|Bengaluru|             1|
|Hyderabad|             1|
|  Chennai|             1|
|  Kolkata|             1|
|    Noida|             1|
|   Indore|             1|
|   Jaipur|             1|
+---------+--------------+

+--------------+-----------------+
|certifications|Each certifcation|
+--------------+-----------------+
|          None|                2|
|           CFP|                1|
|           CFA|                1|
|          CFRM|                1|
|           FRM|                1|
|          CAIA|                1|
|       Series7|                1|
|           CMA|                1|
|           CBP|                1|
+--------------+-----------------+

+--------------------+-------------------+
|      specialization|Total_professionals|
+--------------------+-------------------+
|Corporate Finance...|                  1|
|Retail Banking, W...|                  1|
|Investment Bankin...|                  1|
|Credit Risk, Loan...|                  1|
|Payment Systems, ...|                  1|
|Retail Loans, Mor...|                  1|
|Forex Trading, Fi...|                  1|
|Economic Analysis...|                  1|
|Fintech Solutions...|                  1|
|Private Banking, ...|                  1|
+--------------------+-------------------+

+------------------+--------+
|      current_bank|Max_year|
+------------------+--------+
|         Axis Bank|     5.2|
|    Kotak Mahindra|     3.5|
|        ICICI Bank|    10.0|
|               SBI|     8.0|
|         HDFC Bank|     7.5|
|         Citi Bank|     4.1|
|Standard Chartered|     6.7|
|    Bank of Baroda|     2.8|
|   IDFC First Bank|     9.3|
|               RBI|     7.0|
+------------------+--------+

+---------------+------------+
|education_level|Min_exp_year|
+---------------+------------+
|         B.Tech|         3.5|
|          B.Com|         7.5|
|            BBA|         8.0|
|         M.Tech|         5.2|
|            MBA|         9.3|
|           B.Sc|         4.1|
|          M.Com|         6.7|
|            BCA|         2.8|
|           Ph.D|         7.0|
+---------------+------------+

=================================================

group by profess with current_bank and find top 3 banks with highest avg of exp


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName(" current_bank and find top 3 banks with highest avg of exp - - karthik").getOrCreate()
schemaBanking = StructType([
 StructField("full_name", StringType(), True),
 StructField("experience_years", FloatType(), True),
 StructField("specialization", StringType(), True),
 StructField("location", StringType(), True),
 StructField("education_level", StringType(), True),
 StructField("current_bank", StringType(), True),
 StructField("certifications", StringType(), True)
])
dataBanking = [("Rahul Khanna", 10.0, "Investment Banking, Risk Management", "Mumbai", "MBA", "ICICI Bank", "CFA"),
 ("Meera Desai", 7.5, "Retail Banking, Wealth Management", "Delhi", "B.Com", "HDFC Bank",
"CFP"),
 ("Arjun Malhotra", 5.2, "Corporate Finance, Treasury", "Bengaluru", "M.Tech", "Axis Bank",
"FRM"),
 ("Sneha Kapoor", 8.0, "Credit Risk, Loan Processing", "Hyderabad", "BBA", "SBI", "CFRM"),
 ("Vikram Joshi", 3.5, "Payment Systems, Digital Banking", "Pune", "B.Tech", "Kotak Mahindra",
"None"),
 ("Tanisha Rao", 6.7, "Private Banking, Portfolio Management", "Chennai", "M.Com", "Standard Chartered", "CAIA"),
 ("Rohan Agarwal", 4.1, "Forex Trading, Financial Markets", "Indore", "B.Sc", "Citi Bank", "Series7"),
 ("Neha Saxena", 9.3, "Fintech Solutions, Blockchain Banking", "Noida", "MBA", "IDFC First Bank", "CBP"),
 ("Harish Iyer", 2.8, "Retail Loans, Mortgage Processing", "Jaipur", "BCA", "Bank of Baroda",
"None"),
 ("Kavya Mehta", 7.0, "Economic Analysis, Financial Modeling", "Kolkata", "Ph.D", "RBI", "CMA")
]
df = spark.createDataFrame(dataBanking, schemaBanking)
df.show()
grp_df = df.groupBy(col("current_bank")).agg(avg(col("experience_years")).alias("avg_exp"))
grp_df.show()
grp_df.orderBy(col("avg_exp").desc()).show(3)


+------------------+-----------------+
|      current_bank|          avg_exp|
+------------------+-----------------+
|         Axis Bank|5.199999809265137|
|    Kotak Mahindra|              3.5|
|        ICICI Bank|             10.0|
|               SBI|              8.0|
|         HDFC Bank|              7.5|
|         Citi Bank|4.099999904632568|
|Standard Chartered|6.699999809265137|
|    Bank of Baroda|2.799999952316284|
|   IDFC First Bank|9.300000190734863|
|               RBI|              7.0|
+------------------+-----------------+

+---------------+-----------------+
|   current_bank|          avg_exp|
+---------------+-----------------+
|     ICICI Bank|             10.0|
|IDFC First Bank|9.300000190734863|
|            SBI|              8.0|
+---------------+-----------------+

=============================
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
spark = SparkSession.builder.appName(" grp by location and find bank with most bank professionals - - karthik").getOrCreate()
schemaBanking = StructType([
 StructField("full_name", StringType(), True),
 StructField("experience_years", FloatType(), True),
 StructField("specialization", StringType(), True),
 StructField("location", StringType(), True),
 StructField("education_level", StringType(), True),
 StructField("current_bank", StringType(), True),
 StructField("certifications", StringType(), True)
])
dataBanking = [("Rahul Khanna", 10.0, "Investment Banking, Risk Management", "Mumbai", "MBA", "ICICI Bank", "CFA"),
 ("Meera Desai", 7.5, "Retail Banking, Wealth Management", "Delhi", "B.Com", "HDFC Bank",
"CFP"),
 ("Arjun Malhotra", 5.2, "Corporate Finance, Treasury", "Bengaluru", "M.Tech", "Axis Bank",
"FRM"),
 ("Sneha Kapoor", 8.0, "Credit Risk, Loan Processing", "Hyderabad", "BBA", "SBI", "CFRM"),
 ("Vikram Joshi", 3.5, "Payment Systems, Digital Banking", "Pune", "B.Tech", "Kotak Mahindra",
"None"),
 ("Tanisha Rao", 6.7, "Private Banking, Portfolio Management", "Chennai", "M.Com", "Standard Chartered", "CAIA"),
 ("Rohan Agarwal", 4.1, "Forex Trading, Financial Markets", "Indore", "B.Sc", "Citi Bank", "Series7"),
 ("Neha Saxena", 9.3, "Fintech Solutions, Blockchain Banking", "Noida", "MBA", "IDFC First Bank", "CBP"),
 ("Harish Iyer", 2.8, "Retail Loans, Mortgage Processing", "Jaipur", "BCA", "Bank of Baroda",
"None"),
 ("Kavya Mehta", 7.0, "Economic Analysis, Financial Modeling", "Kolkata", "Ph.D", "RBI", "CMA")
]
df = spark.createDataFrame(dataBanking, schemaBanking)
df.show()
grp_df = df.groupBy(col("location"),col("current_bank")).agg(count(col("*")).alias("most_bank"))
grp_df.show()
window_spec = Window.orderBy(col("most_bank").desc())
rs_df = grp_df.withColumn("rank",rank().over(window_spec))
rs_df.show()

rs_df.filter(col("rank")==1).show()

+---------+------------------+---------+
| location|      current_bank|most_bank|
+---------+------------------+---------+
|     Pune|    Kotak Mahindra|        1|
|    Delhi|         HDFC Bank|        1|
|Hyderabad|               SBI|        1|
|Bengaluru|         Axis Bank|        1|
|   Mumbai|        ICICI Bank|        1|
|  Chennai|Standard Chartered|        1|
|  Kolkata|               RBI|        1|
|   Indore|         Citi Bank|        1|
|    Noida|   IDFC First Bank|        1|
|   Jaipur|    Bank of Baroda|        1|
+---------+------------------+---------+

+---------+------------------+---------+----+
| location|      current_bank|most_bank|rank|
+---------+------------------+---------+----+
|Bengaluru|         Axis Bank|        1|   1|
|  Chennai|Standard Chartered|        1|   1|
|    Delhi|         HDFC Bank|        1|   1|
|Hyderabad|               SBI|        1|   1|
|   Indore|         Citi Bank|        1|   1|
|   Jaipur|    Bank of Baroda|        1|   1|
|  Kolkata|               RBI|        1|   1|
|   Mumbai|        ICICI Bank|        1|   1|
|    Noida|   IDFC First Bank|        1|   1|
|     Pune|    Kotak Mahindra|        1|   1|
+---------+------------------+---------+----+

+---------+------------------+---------+----+
| location|      current_bank|most_bank|rank|
+---------+------------------+---------+----+
|Bengaluru|         Axis Bank|        1|   1|
|  Chennai|Standard Chartered|        1|   1|
|    Delhi|         HDFC Bank|        1|   1|
|Hyderabad|               SBI|        1|   1|
|   Indore|         Citi Bank|        1|   1|
|   Jaipur|    Bank of Baroda|        1|   1|
|  Kolkata|               RBI|        1|   1|
|   Mumbai|        ICICI Bank|        1|   1|
|    Noida|   IDFC First Bank|        1|   1|
|     Pune|    Kotak Mahindra|        1|   1|
+---------+------------------+---------+----+
==============================================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Date functions").getOrCreate()
#Calculate the difference
data_date =[("2023-01-01", "2023-02-01"),
    ("2023-03-15", "2023-03-20")]
columns_date = ["start_date","end_date"]
df = spark.createDataFrame(data_date,columns_date)
df.show()
df.printSchema()
df = df.withColumn("start_date",to_date(col("start_date"))).withColumn("end_date",to_date(col("end_date")))
df.printSchema()
date_dff_df = df.withColumn("Date_diff",datediff(col("end_date"),col("start_date")))
date_dff_df.show()

+----------+----------+
|start_date|  end_date|
+----------+----------+
|2023-01-01|2023-02-01|
|2023-03-15|2023-03-20|
+----------+----------+

root
 |-- start_date: string (nullable = true)
 |-- end_date: string (nullable = true)

root
 |-- start_date: date (nullable = true)
 |-- end_date: date (nullable = true)

+----------+----------+---------+
|start_date|  end_date|Date_diff|
+----------+----------+---------+
|2023-01-01|2023-02-01|       31|
|2023-03-15|2023-03-20|        5|
+----------+----------+---------+

====
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Extracting Year from a Date Column").getOrCreate()
#Calculate the difference
data_date =[("2023-01-01",) ,("2023-02-01",),
    ("2023-03-15",), ("2023-03-20",)]
columns_date = ["start_date"]
df = spark.createDataFrame(data_date,columns_date)
df.show()
df.printSchema()
#df = df.withColumn("start_date",to_date(col("start_date"))).withColumn("end_date",to_date(col("end_date")))
df= df.withColumn("start_date",to_date(col("start_date"),"yyyy-MM-dd"))
df.printSchema()
extract_df = df.withColumn("year",year(col("start_date")))

extract_df.show()

+----------+----+
|start_date|year|
+----------+----+
|2023-01-01|2023|
|2023-02-01|2023|
|2023-03-15|2023|
|2023-03-20|2023|

===================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Filter Records Based on Date").getOrCreate()

#Calculate the difference
data_date =[("2023-01-01",) ,("2023-02-01",),
    ("2023-03-15",), ("2023-03-20",)]
columns_date = ["start_date"]
df = spark.createDataFrame(data_date,columns_date)
df.show()
df.printSchema()
df.filter(col("start_date")> "2023-02-01").show()

+----------+
|start_date|
+----------+
|2023-01-01|
|2023-02-01|
|2023-03-15|
|2023-03-20|
+----------+

root
 |-- start_date: string (nullable = true)

+----------+
|start_date|
+----------+
|2023-03-15|
|2023-03-20|

=================
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("max Date").getOrCreate()

#Calculate the difference
data_date =[("2023-01-01",) ,("2023-02-01",),
    ("2023-03-15",), ("2023-03-20",)]
columns_date = ["start_date"]
df = spark.createDataFrame(data_date,columns_date)
df.show()
df.printSchema()
add_date = df.agg(max(col("start_date")).alias("max_date"))
add_date.show()

+----------+
|  max_date|
+----------+
|2023-03-20|
+----------+

=========

Use Case:

You have two tables:
	1.	transactions: Contains transaction details.
	•	Columns: id, user_id, transaction_value, created_at (DateTime).
	2.	users: Contains user information.
	•	Columns: user_id, name, email, created_at (DateTime when the user registered).
# Sample transactions data
transactions_data = [
    (1, 101, 150, "2024-01-01 10:00:00"),
    (2, 101, 200, "2024-01-01 11:00:00"),
    (3, 102, 50, "2024-01-02 12:00:00"),
    (4, 103, 300, "2024-01-03 13:00:00")
]

transactions_columns = ["id", "user_id", "transaction_value", "created_at"]
# Sample users data
users_data = [
    (101, "Alice", "alice@example.com", "2023-02-01"),
    (102, "Bob", "bob@example.com", "2023-01-01"),
    (103, "Charlie", "charlie@example.com", "2023-03-01")
]

users_columns = ["user_id", "name", "email", "created_at"]
Problem:

Find the latest transaction per day for each user, 
including the user’s name and email. 
Additionally, 
filter for transactions where the transaction_value is greater than 100, and 
include only users who registered after 2023-01-01.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("max Date").getOrCreate()
# Sample transactions data
transactions_data = [
    (1, 101, 150, "2024-01-01 10:00:00"),
    (2, 101, 200, "2024-01-01 11:00:00"),
    (3, 102, 50, "2024-01-02 12:00:00"),
    (4, 103, 300, "2024-01-03 13:00:00")
]

transactions_columns = ["id", "user_id", "transaction_value", "created_at"]
# Sample users data
users_data = [
    (101, "Alice", "alice@example.com", "2023-02-01"),
    (102, "Bob", "bob@example.com", "2023-01-01"),
    (103, "Charlie", "charlie@example.com", "2023-03-01")
]

users_columns = ["user_id", "name", "email", "created_at"]
trans_df = spark.createDataFrame(transactions_data,transactions_columns)
trans_df.show()
users_df = spark.createDataFrame(users_data,users_columns)
users_df.show()
filter_trans_df = trans_df.filter(col("transaction_value")>100)
filter_trans_df.show()
users_date_df= users_df.filter(col("created_at")>"2023-01-01")
users_date_df.show()
window_spec = Window.partitionBy(users_df.user_id).orderBy(filter_trans_df.created_at.desc())
per_df = filter_trans_df.join(users_date_df,"user_id","inner").select(users_date_df.name,users_date_df.email,filter_trans_df.transaction_value,dense_rank().over(window_spec).alias("rnk"))
per_df.show()
per_df = per_df.filter(col("rnk")==1)
per_df.show()



+---+-------+-----------------+-------------------+
| id|user_id|transaction_value|         created_at|
+---+-------+-----------------+-------------------+
|  1|    101|              150|2024-01-01 10:00:00|
|  2|    101|              200|2024-01-01 11:00:00|
|  3|    102|               50|2024-01-02 12:00:00|
|  4|    103|              300|2024-01-03 13:00:00|
+---+-------+-----------------+-------------------+

+-------+-------+-------------------+----------+
|user_id|   name|              email|created_at|
+-------+-------+-------------------+----------+
|    101|  Alice|  alice@example.com|2023-02-01|
|    102|    Bob|    bob@example.com|2023-01-01|
|    103|Charlie|charlie@example.com|2023-03-01|
+-------+-------+-------------------+----------+

+---+-------+-----------------+-------------------+
| id|user_id|transaction_value|         created_at|
+---+-------+-----------------+-------------------+
|  1|    101|              150|2024-01-01 10:00:00|
|  2|    101|              200|2024-01-01 11:00:00|
|  4|    103|              300|2024-01-03 13:00:00|
+---+-------+-----------------+-------------------+

+-------+-------+-------------------+----------+
|user_id|   name|              email|created_at|
+-------+-------+-------------------+----------+
|    101|  Alice|  alice@example.com|2023-02-01|
|    103|Charlie|charlie@example.com|2023-03-01|
+-------+-------+-------------------+----------+

+-------+-------------------+-----------------+---+
|   name|              email|transaction_value|rnk|
+-------+-------------------+-----------------+---+
|  Alice|  alice@example.com|              200|  1|
|  Alice|  alice@example.com|              150|  2|
|Charlie|charlie@example.com|              300|  1|
+-------+-------------------+-----------------+---+

+-------+-------------------+-----------------+---+
|   name|              email|transaction_value|rnk|
+-------+-------------------+-----------------+---+
|  Alice|  alice@example.com|              200|  1|
|Charlie|charlie@example.com|              300|  1|
+-------+-------------------+-----------------+---+


==============

Truncate Date to the First Day of the Month

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType

spark = SparkSession.builder.master("local[*]").appName("Truncate Date").getOrCreate()

# Define the schema
schema = StructType([
    StructField("sale_date", StringType(), True)  
])

# Create DataFrame with string values
df = spark.createDataFrame([
    ("2023-04-12",),
    ("2023-07-23",),
    ("2023-08-05",)
], schema)
df.show()
# truncate date to the first day of the month
first_day_df = df.withColumn("first_day_of_the_month",trunc(col("sale_date"),"MM"))
first_day_df.show()

+----------+
| sale_date|
+----------+
|2023-04-12|
|2023-07-23|
|2023-08-05|
+----------+

+----------+----------------------+
| sale_date|first_day_of_the_month|
+----------+----------------------+
|2023-04-12|            2023-04-01|
|2023-07-23|            2023-07-01|
|2023-08-05|            2023-08-01|
+----------+----------------------+

==================

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType

spark = SparkSession.builder.master("local[*]").appName("Records By Year").getOrCreate()

# Define the schema
schema = StructType([
    StructField("transaction_date", StringType(), True)  
])

# Create DataFrame with string values
df = spark.createDataFrame([
    ("2023-06-12",),
    ("2022-11-09",),
    ("2021-04-01",)
], schema)
df.show()
res_df = df.withColumn("groupBy_year",year(col("transaction_date"))).groupBy(col("groupBy_year")).agg(count("*").alias("Total"))
res_df.show()

+----------------+
|transaction_date|
+----------------+
|      2023-06-12|
|      2022-11-09|
|      2021-04-01|
+----------------+

+------------+-----+
|groupBy_year|Total|
+------------+-----+
|        2023|    1|
|        2022|    1|
|        2021|    1|
+------------+-----+

===============
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType

spark = SparkSession.builder.master("local[*]").appName("Day of Week").getOrCreate()

# Create DataFrame with string values
df = spark.createDataFrame([
    ("2023-08-11",),
    ("2023-07-25",),
    ("2023-09-01",)
], ["attendance_date"])

# Cast 'attendance_date' to DateType and calculate the day of the week
df= df.withColumn("day of the week",dayofweek(col("attendance_date")))
df.show()

+---------------+---------------+
|attendance_date|day of the week|
+---------------+---------------+
|     2023-08-11|              6|
|     2023-07-25|              3|
|     2023-09-01|              6|

=============
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("leap year").getOrCreate()

# Create DataFrame with string values
df = spark.createDataFrame([
    ("2020-03-01",),
    ("2019-12-15",),
    ("2024-02-29",)
], ["birth_date"])

# Convert 'birth_date' to DateType and check for leap year
df= df.withColumn("birth_date",to_date(col("birth_date")))
df.printSchema()
leap_df = df.withColumn("Leap Year",(year(col("birth_date")) % 4 == 0) & (year(col("birth_date")) % 100 != 0) | (year(col("birth_date")) % 400 == 0) )
leap_df.show()


=======================
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("max Date").getOrCreate()
dataBanking = [
    ("Rahul Khanna", 10.0, "Investment Banking, Risk Management", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Meera Desai", 7.5, "Investment Banking, Wealth Management", "Delhi", "B.Com", "HDFC Bank", "CFP"),
    ("Arjun Malhotra", 5.2, "Corporate Finance, Treasury", "Bengaluru", "M.Tech", "Axis Bank", "FRM"),
    ("Sneha Kapoor", 8.0, "Credit Risk, Loan Processing", "Hyderabad", "BBA", "SBI", "CFRM"),
    ("Vikram Joshi", 3.5, "Payment Systems, Digital Banking", "Mumbai", "B.Tech", "Kotak Mahindra", "CFA"),
    ("Tanisha Rao", 6.7, "Private Banking, Portfolio Management", "Chennai", "M.Com", "Standard Chartered", "CFP"),
    ("Rohan Agarwal", 4.1, "Forex Trading, Financial Markets", "Indore", "B.Sc", "Citi Bank", "Series 7"),
    ("Neha Saxena", 9.3, "Fintech Solutions, Blockchain Banking", "Noida", "MBA", "RBI", "CBP"),
    ("Harish Iyer", 2.8, "Retail Loans, Mortgage Processing", "Jaipur", "BCA", "Bank of Baroda", "None"),
    ("Kavya Mehta", 7.0, "Economic Analysis, Financial Modeling", "Kolkata", "Ph.D", "RBI", "CMA"),
    ("Aditya Verma", 6.2, "Investment Banking, Mergers & Acquisitions", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Priya Sharma", 5.8, "Risk Management, Financial Analysis", "Bangalore", "M.Com", "Axis Bank", "FRM"),
    ("Kunal Bansal", 4.5, "Wealth Management, Portfolio Strategy", "Delhi", "BBA", "HDFC Bank", "CFP"),
    ("Anjali Nair", 7.9, "Loan Processing, Credit Assessment", "Hyderabad", "B.Sc", "SBI", "CFRM"),
    ("Amit Reddy", 8.4, "Corporate Treasury, Debt Management", "Chennai", "M.Tech", "Standard Chartered", "FRM")
]
schemaBanking = StructType([
    StructField("full_name", StringType(), True),
    StructField("experience_years", FloatType(), True),
    StructField("specialization", StringType(), True),
    StructField("location", StringType(), True),
    StructField("education_level", StringType(),True),
    StructField("current_bank", StringType(),True),
    StructField("certifications", StringType(),True)
])
dfBanking = spark.createDataFrame(dataBanking, schema=schemaBanking)
dfBanking.show()
#Find the average experience per bank
bnk_avg_df = dfBanking.groupBy(col("current_bank")).agg(avg(col("experience_years")).alias("avg_exp"))
bnk_avg_df.orderBy(desc("avg_exp")).show()
#Calculate the average experience per specialization, but only include specializations where the average is above 6 years.
spz_avg_df = dfBanking.groupBy(col("specialization")).agg(avg(col("experience_years")))
spz_avg_df.show()
spz_avg_df.filter(col("avg(experience_years)")>6).show()

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("CountDistinct for Unique Entries").getOrCreate()
dataBanking = [
    ("Rahul Khanna", 10.0, "Investment Banking, Risk Management", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Meera Desai", 7.5, "Investment Banking, Wealth Management", "Delhi", "B.Com", "HDFC Bank", "CFP"),
    ("Arjun Malhotra", 5.2, "Corporate Finance, Treasury", "Bengaluru", "M.Tech", "Axis Bank", "FRM"),
    ("Sneha Kapoor", 8.0, "Credit Risk, Loan Processing", "Hyderabad", "BBA", "SBI", "CFRM"),
    ("Vikram Joshi", 3.5, "Payment Systems, Digital Banking", "Mumbai", "B.Tech", "Kotak Mahindra", "CFA"),
    ("Tanisha Rao", 6.7, "Private Banking, Portfolio Management", "Chennai", "M.Com", "Standard Chartered", "CFP"),
    ("Rohan Agarwal", 4.1, "Forex Trading, Financial Markets", "Indore", "B.Sc", "Citi Bank", "Series 7"),
    ("Neha Saxena", 9.3, "Fintech Solutions, Blockchain Banking", "Noida", "MBA", "RBI", "CBP"),
    ("Harish Iyer", 2.8, "Retail Loans, Mortgage Processing", "Jaipur", "BCA", "Bank of Baroda", "None"),
    ("Kavya Mehta", 7.0, "Economic Analysis, Financial Modeling", "Kolkata", "Ph.D", "RBI", "CMA"),
    ("Aditya Verma", 6.2, "Investment Banking, Mergers & Acquisitions", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Priya Sharma", 5.8, "Risk Management, Financial Analysis", "Bangalore", "M.Com", "Axis Bank", "FRM"),
    ("Kunal Bansal", 4.5, "Wealth Management, Portfolio Strategy", "Delhi", "BBA", "HDFC Bank", "CFP"),
    ("Anjali Nair", 7.9, "Loan Processing, Credit Assessment", "Hyderabad", "B.Sc", "SBI", "CFRM"),
    ("Amit Reddy", 8.4, "Corporate Treasury, Debt Management", "Chennai", "M.Tech", "Standard Chartered", "FRM")
]
schemaBanking = StructType([
    StructField("full_name", StringType(), True),
    StructField("experience_years", FloatType(), True),
    StructField("specialization", StringType(), True),
    StructField("location", StringType(), True),
    StructField("education_level", StringType(),True),
    StructField("current_bank", StringType(),True),
    StructField("certifications", StringType(),True)
])
dfBanking = spark.createDataFrame(dataBanking, schema=schemaBanking)
dfBanking.show()
#Find the number of distinct certifications held per bank.
df_Bank = dfBanking.groupBy(col("current_bank")).agg(countDistinct(col("certifications")).alias("distinct_certifications"))
df_Bank.orderBy(desc(col("distinct_certifications"))).show()
#Count the number of different education levels per city.
df_educ=dfBanking.groupBy(col("location")).agg(count_distinct(col("education_level")).alias("distinct_education_levels"))
df_educ.orderBy(desc("distinct_education_levels")).show()

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Filtering on Aggregated Data").getOrCreate()
dataBanking = [
    ("Rahul Khanna", 10.0, "Investment Banking, Risk Management", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Meera Desai", 7.5, "Investment Banking, Wealth Management", "Delhi", "B.Com", "HDFC Bank", "CFP"),
    ("Arjun Malhotra", 5.2, "Corporate Finance, Treasury", "Bengaluru", "M.Tech", "Axis Bank", "FRM"),
    ("Sneha Kapoor", 8.0, "Credit Risk, Loan Processing", "Hyderabad", "BBA", "SBI", "CFRM"),
    ("Vikram Joshi", 3.5, "Payment Systems, Digital Banking", "Mumbai", "B.Tech", "Kotak Mahindra", "CFA"),
    ("Tanisha Rao", 6.7, "Private Banking, Portfolio Management", "Chennai", "M.Com", "Standard Chartered", "CFP"),
    ("Rohan Agarwal", 4.1, "Forex Trading, Financial Markets", "Indore", "B.Sc", "Citi Bank", "Series 7"),
    ("Neha Saxena", 9.3, "Fintech Solutions, Blockchain Banking", "Noida", "MBA", "RBI", "CBP"),
    ("Harish Iyer", 2.8, "Retail Loans, Mortgage Processing", "Jaipur", "BCA", "Bank of Baroda", "None"),
    ("Kavya Mehta", 7.0, "Economic Analysis, Financial Modeling", "Kolkata", "Ph.D", "RBI", "CMA"),
    ("Aditya Verma", 6.2, "Investment Banking, Mergers & Acquisitions", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Priya Sharma", 5.8, "Risk Management, Financial Analysis", "Bangalore", "M.Com", "Axis Bank", "FRM"),
    ("Kunal Bansal", 4.5, "Wealth Management, Portfolio Strategy", "Delhi", "BBA", "HDFC Bank", "CFP"),
    ("Anjali Nair", 7.9, "Loan Processing, Credit Assessment", "Hyderabad", "B.Sc", "SBI", "CFRM"),
    ("Amit Reddy", 8.4, "Corporate Treasury, Debt Management", "Chennai", "M.Tech", "Standard Chartered", "FRM")
]
schemaBanking = StructType([
    StructField("full_name", StringType(), True),
    StructField("experience_years", FloatType(), True),
    StructField("specialization", StringType(), True),
    StructField("location", StringType(), True),
    StructField("education_level", StringType(),True),
    StructField("current_bank", StringType(),True),
    StructField("certifications", StringType(),True)
])
dfBanking = spark.createDataFrame(dataBanking, schema=schemaBanking)
dfBanking.show()
#Find banks where professionals have at least 7 years of average experience.
df_bank = dfBanking.groupBy(col("current_bank"),col("full_name")).agg(avg(col("experience_years")).alias("avg_exp")).filter(col("avg_exp")>7)
df_bank.orderBy(desc(col("avg_exp"))).show()
#Identify certifications that appear more than once in the dataset.
cert_df = dfBanking.groupBy(col("certifications")).agg(count(col("full_name")).alias("more_than_once"))
cert_df.show()
cert_df.filter(col("more_than_once")>1).show()

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Sum Aggregation").getOrCreate()
dataBanking = [
    ("Rahul Khanna", 10.0, "Investment Banking, Risk Management", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Meera Desai", 7.5, "Investment Banking, Wealth Management", "Delhi", "B.Com", "HDFC Bank", "CFP"),
    ("Arjun Malhotra", 5.2, "Corporate Finance, Treasury", "Bengaluru", "M.Tech", "Axis Bank", "FRM"),
    ("Sneha Kapoor", 8.0, "Credit Risk, Loan Processing", "Hyderabad", "BBA", "SBI", "CFRM"),
    ("Vikram Joshi", 3.5, "Payment Systems, Digital Banking", "Mumbai", "B.Tech", "Kotak Mahindra", "CFA"),
    ("Tanisha Rao", 6.7, "Private Banking, Portfolio Management", "Chennai", "M.Com", "Standard Chartered", "CFP"),
    ("Rohan Agarwal", 4.1, "Forex Trading, Financial Markets", "Indore", "B.Sc", "Citi Bank", "Series 7"),
    ("Neha Saxena", 9.3, "Fintech Solutions, Blockchain Banking", "Noida", "MBA", "RBI", "CBP"),
    ("Harish Iyer", 2.8, "Retail Loans, Mortgage Processing", "Jaipur", "BCA", "Bank of Baroda", "None"),
    ("Kavya Mehta", 7.0, "Economic Analysis, Financial Modeling", "Kolkata", "Ph.D", "RBI", "CMA"),
    ("Aditya Verma", 6.2, "Investment Banking, Mergers & Acquisitions", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Priya Sharma", 5.8, "Risk Management, Financial Analysis", "Bangalore", "M.Com", "Axis Bank", "FRM"),
    ("Kunal Bansal", 4.5, "Wealth Management, Portfolio Strategy", "Delhi", "BBA", "HDFC Bank", "CFP"),
    ("Anjali Nair", 7.9, "Loan Processing, Credit Assessment", "Hyderabad", "B.Sc", "SBI", "CFRM"),
    ("Amit Reddy", 8.4, "Corporate Treasury, Debt Management", "Chennai", "M.Tech", "Standard Chartered", "FRM")
]
schemaBanking = StructType([
    StructField("full_name", StringType(), True),
    StructField("experience_years", FloatType(), True),
    StructField("specialization", StringType(), True),
    StructField("location", StringType(), True),
    StructField("education_level", StringType(),True),
    StructField("current_bank", StringType(),True),
    StructField("certifications", StringType(),True)
])
dfBanking = spark.createDataFrame(dataBanking, schema=schemaBanking)
dfBanking.show()
#Calculate total experience per specialization, but only show specializations where total experience exceeds 5 years.
df_spec = dfBanking.groupBy(col("specialization")).agg(sum(col("experience_years")).alias("total_exp"))
df_spec.show()
df_spec.filter(col("total_exp")>5).orderBy(desc(col("total_exp"))).show()
#10.Find the total and average experience per bank.
total_avg_df = dfBanking.groupBy(col("current_bank")).agg(sum(col("experience_years")).alias("total_exp"),avg(col("experience_years")).alias("avg_exp"))
total_avg_df.orderBy(desc(col("avg_exp"))).show()

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Max & Min Aggregation").getOrCreate()
dataBanking = [
    ("Rahul Khanna", 10.0, "Investment Banking, Risk Management", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Meera Desai", 7.5, "Investment Banking, Wealth Management", "Delhi", "B.Com", "HDFC Bank", "CFP"),
    ("Arjun Malhotra", 5.2, "Corporate Finance, Treasury", "Bengaluru", "M.Tech", "Axis Bank", "FRM"),
    ("Sneha Kapoor", 8.0, "Credit Risk, Loan Processing", "Hyderabad", "BBA", "SBI", "CFRM"),
    ("Vikram Joshi", 3.5, "Payment Systems, Digital Banking", "Mumbai", "B.Tech", "Kotak Mahindra", "CFA"),
    ("Tanisha Rao", 6.7, "Private Banking, Portfolio Management", "Chennai", "M.Com", "Standard Chartered", "CFP"),
    ("Rohan Agarwal", 4.1, "Forex Trading, Financial Markets", "Indore", "B.Sc", "Citi Bank", "Series 7"),
    ("Neha Saxena", 9.3, "Fintech Solutions, Blockchain Banking", "Noida", "MBA", "RBI", "CBP"),
    ("Harish Iyer", 2.8, "Retail Loans, Mortgage Processing", "Jaipur", "BCA", "Bank of Baroda", "None"),
    ("Kavya Mehta", 7.0, "Economic Analysis, Financial Modeling", "Kolkata", "Ph.D", "RBI", "CMA"),
    ("Aditya Verma", 6.2, "Investment Banking, Mergers & Acquisitions", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Priya Sharma", 5.8, "Risk Management, Financial Analysis", "Bangalore", "M.Com", "Axis Bank", "FRM"),
    ("Kunal Bansal", 4.5, "Wealth Management, Portfolio Strategy", "Delhi", "BBA", "HDFC Bank", "CFP"),
    ("Anjali Nair", 7.9, "Loan Processing, Credit Assessment", "Hyderabad", "B.Sc", "SBI", "CFRM"),
    ("Amit Reddy", 8.4, "Corporate Treasury, Debt Management", "Chennai", "M.Tech", "Standard Chartered", "FRM")
]
schemaBanking = StructType([
    StructField("full_name", StringType(), True),
    StructField("experience_years", FloatType(), True),
    StructField("specialization", StringType(), True),
    StructField("location", StringType(), True),
    StructField("education_level", StringType(),True),
    StructField("current_bank", StringType(),True),
    StructField("certifications", StringType(),True)
])
dfBanking = spark.createDataFrame(dataBanking, schema=schemaBanking)
dfBanking.show()
#11 Find max and min experience per specialization.
df_spec = dfBanking.groupBy(col("specialization")).agg(max(col("experience_years")).alias("Max_exp"),min(col("experience_years")).alias("min_exp"))
df_spec.show()
#12 Get the most experienced professional per bank.
df_most = dfBanking.groupBy(col("current_bank")).agg(max(col("experience_years")).alias("max_exp"))
df_most.show()

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Counting Unique Groups").getOrCreate()
dataBanking = [
    ("Rahul Khanna", 10.0, "Investment Banking, Risk Management", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Meera Desai", 7.5, "Investment Banking, Wealth Management", "Delhi", "B.Com", "HDFC Bank", "CFP"),
    ("Arjun Malhotra", 5.2, "Corporate Finance, Treasury", "Bengaluru", "M.Tech", "Axis Bank", "FRM"),
    ("Sneha Kapoor", 8.0, "Credit Risk, Loan Processing", "Hyderabad", "BBA", "SBI", "CFRM"),
    ("Vikram Joshi", 3.5, "Payment Systems, Digital Banking", "Mumbai", "B.Tech", "Kotak Mahindra", "CFA"),
    ("Tanisha Rao", 6.7, "Private Banking, Portfolio Management", "Chennai", "M.Com", "Standard Chartered", "CFP"),
    ("Rohan Agarwal", 4.1, "Forex Trading, Financial Markets", "Indore", "B.Sc", "Citi Bank", "Series 7"),
    ("Neha Saxena", 9.3, "Fintech Solutions, Blockchain Banking", "Noida", "MBA", "RBI", "CBP"),
    ("Harish Iyer", 2.8, "Retail Loans, Mortgage Processing", "Jaipur", "BCA", "Bank of Baroda", "None"),
    ("Kavya Mehta", 7.0, "Economic Analysis, Financial Modeling", "Kolkata", "Ph.D", "RBI", "CMA"),
    ("Aditya Verma", 6.2, "Investment Banking, Mergers & Acquisitions", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Priya Sharma", 5.8, "Risk Management, Financial Analysis", "Bangalore", "M.Com", "Axis Bank", "FRM"),
    ("Kunal Bansal", 4.5, "Wealth Management, Portfolio Strategy", "Delhi", "BBA", "HDFC Bank", "CFP"),
    ("Anjali Nair", 7.9, "Loan Processing, Credit Assessment", "Hyderabad", "B.Sc", "SBI", "CFRM"),
    ("Amit Reddy", 8.4, "Corporate Treasury, Debt Management", "Chennai", "M.Tech", "Standard Chartered", "FRM")
]
schemaBanking = StructType([
    StructField("full_name", StringType(), True),
    StructField("experience_years", FloatType(), True),
    StructField("specialization", StringType(), True),
    StructField("location", StringType(), True),
    StructField("education_level", StringType(),True),
    StructField("current_bank", StringType(),True),
    StructField("certifications", StringType(),True)
])
dfBanking = spark.createDataFrame(dataBanking, schema=schemaBanking)
dfBanking.show()
#13 Show cities where there are more than 2 different banks.
df_spec = dfBanking.groupBy(col("location")).agg(countDistinct(col("current_bank")).alias("diff_cities"))
df_spec.show()
df_spec.filter(col("diff_cities")>2).show()
#14Find banks where professionals hold certifications and have at least 7 years of experience.
df_Bank = dfBanking.filter((col("certifications").isNotNull()) & (col("experience_years")>7)).select(col("current_bank"),col("certifications"),col("experience_years"))
df_Bank.show()

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Conditional GroupBy Queries").getOrCreate()
dataBanking = [
    ("Rahul Khanna", 10.0, "Investment Banking, Risk Management", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Meera Desai", 7.5, "Investment Banking, Wealth Management", "Delhi", "B.Com", "HDFC Bank", "CFP"),
    ("Arjun Malhotra", 5.2, "Corporate Finance, Treasury", "Bengaluru", "M.Tech", "Axis Bank", "FRM"),
    ("Sneha Kapoor", 8.0, "Credit Risk, Loan Processing", "Hyderabad", "BBA", "SBI", "CFRM"),
    ("Vikram Joshi", 3.5, "Payment Systems, Digital Banking", "Mumbai", "B.Tech", "Kotak Mahindra", "CFA"),
    ("Tanisha Rao", 6.7, "Private Banking, Portfolio Management", "Chennai", "M.Com", "Standard Chartered", "CFP"),
    ("Rohan Agarwal", 4.1, "Forex Trading, Financial Markets", "Indore", "B.Sc", "Citi Bank", "Series 7"),
    ("Neha Saxena", 9.3, "Fintech Solutions, Blockchain Banking", "Noida", "MBA", "RBI", "CBP"),
    ("Harish Iyer", 2.8, "Retail Loans, Mortgage Processing", "Jaipur", "BCA", "Bank of Baroda", "None"),
    ("Kavya Mehta", 7.0, "Economic Analysis, Financial Modeling", "Kolkata", "Ph.D", "RBI", "CMA"),
    ("Aditya Verma", 6.2, "Investment Banking, Mergers & Acquisitions", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Priya Sharma", 5.8, "Risk Management, Financial Analysis", "Bangalore", "M.Com", "Axis Bank", "FRM"),
    ("Kunal Bansal", 4.5, "Wealth Management, Portfolio Strategy", "Delhi", "BBA", "HDFC Bank", "CFP"),
    ("Anjali Nair", 7.9, "Loan Processing, Credit Assessment", "Hyderabad", "B.Sc", "SBI", "CFRM"),
    ("Amit Reddy", 8.4, "Corporate Treasury, Debt Management", "Chennai", "M.Tech", "Standard Chartered", "FRM")
]
schemaBanking = StructType([
    StructField("full_name", StringType(), True),
    StructField("experience_years", FloatType(), True),
    StructField("specialization", StringType(), True),
    StructField("location", StringType(), True),
    StructField("education_level", StringType(),True),
    StructField("current_bank", StringType(),True),
    StructField("certifications", StringType(),True)
])
dfBanking = spark.createDataFrame(dataBanking, schema=schemaBanking)
dfBanking.show()
#15 Find the specialization with the most professionals holding certifications.
df_spec = dfBanking.groupBy(col("specialization")).agg(count(col("certifications")).alias("total_certifications"))
df_spec.orderBy(desc(col("total_certifications"))).show()
#16 Identify banks where professionals have at least 2 distinct certifications.
df_cert = dfBanking.groupBy(col("current_bank"),col("certifications")).agg(count(col("full_name")).alias("professionals"))
df_cert.filter(col("professionals")>2).orderBy(desc(col("professionals"))).show()

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Multi-Aggregation with Different Functions").getOrCreate()
dataBanking = [
    ("Rahul Khanna", 10.0, "Investment Banking, Risk Management", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Meera Desai", 7.5, "Investment Banking, Wealth Management", "Delhi", "B.Com", "HDFC Bank", "CFP"),
    ("Arjun Malhotra", 5.2, "Corporate Finance, Treasury", "Bengaluru", "M.Tech", "Axis Bank", "FRM"),
    ("Sneha Kapoor", 8.0, "Credit Risk, Loan Processing", "Hyderabad", "BBA", "SBI", "CFRM"),
    ("Vikram Joshi", 3.5, "Payment Systems, Digital Banking", "Mumbai", "B.Tech", "Kotak Mahindra", "CFA"),
    ("Tanisha Rao", 6.7, "Private Banking, Portfolio Management", "Chennai", "M.Com", "Standard Chartered", "CFP"),
    ("Rohan Agarwal", 4.1, "Forex Trading, Financial Markets", "Indore", "B.Sc", "Citi Bank", "Series 7"),
    ("Neha Saxena", 9.3, "Fintech Solutions, Blockchain Banking", "Noida", "MBA", "RBI", "CBP"),
    ("Harish Iyer", 2.8, "Retail Loans, Mortgage Processing", "Jaipur", "BCA", "Bank of Baroda", "None"),
    ("Kavya Mehta", 7.0, "Economic Analysis, Financial Modeling", "Kolkata", "Ph.D", "RBI", "CMA"),
    ("Aditya Verma", 6.2, "Investment Banking, Mergers & Acquisitions", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Priya Sharma", 5.8, "Risk Management, Financial Analysis", "Bangalore", "M.Com", "Axis Bank", "FRM"),
    ("Kunal Bansal", 4.5, "Wealth Management, Portfolio Strategy", "Delhi", "BBA", "HDFC Bank", "CFP"),
    ("Anjali Nair", 7.9, "Loan Processing, Credit Assessment", "Hyderabad", "B.Sc", "SBI", "CFRM"),
    ("Amit Reddy", 8.4, "Corporate Treasury, Debt Management", "Chennai", "M.Tech", "Standard Chartered", "FRM")
]
schemaBanking = StructType([
    StructField("full_name", StringType(), True),
    StructField("experience_years", FloatType(), True),
    StructField("specialization", StringType(), True),
    StructField("location", StringType(), True),
    StructField("education_level", StringType(),True),
    StructField("current_bank", StringType(),True),
    StructField("certifications", StringType(),True)
])
dfBanking = spark.createDataFrame(dataBanking, schema=schemaBanking)
dfBanking.show()
#17 Calculate the total, average, and maximum experience per bank.
df_bank = dfBanking.groupBy(col("current_bank")).agg(sum(col("experience_years")),avg(col("experience_years")),max(col("experience_years")))
df_bank.show()
#18 Find the total, average, and minimum experience per specialization.
df_spec = dfBanking.groupBy(col("specialization")).agg(sum(col("experience_years")),avg(col("experience_years")),min(col("experience_years")))
df_spec.show()

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Aggregation with Filtering and Sorting").getOrCreate()
dataBanking = [
    ("Rahul Khanna", 10.0, "Investment Banking, Risk Management", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Meera Desai", 7.5, "Investment Banking, Wealth Management", "Delhi", "B.Com", "HDFC Bank", "CFP"),
    ("Arjun Malhotra", 5.2, "Corporate Finance, Treasury", "Bengaluru", "M.Tech", "Axis Bank", "FRM"),
    ("Sneha Kapoor", 8.0, "Credit Risk, Loan Processing", "Hyderabad", "BBA", "SBI", "CFRM"),
    ("Vikram Joshi", 3.5, "Payment Systems, Digital Banking", "Mumbai", "B.Tech", "Kotak Mahindra", "CFA"),
    ("Tanisha Rao", 6.7, "Private Banking, Portfolio Management", "Chennai", "M.Com", "Standard Chartered", "CFP"),
    ("Rohan Agarwal", 4.1, "Forex Trading, Financial Markets", "Indore", "B.Sc", "Citi Bank", "Series 7"),
    ("Neha Saxena", 9.3, "Fintech Solutions, Blockchain Banking", "Noida", "MBA", "RBI", "CBP"),
    ("Harish Iyer", 2.8, "Retail Loans, Mortgage Processing", "Jaipur", "BCA", "Bank of Baroda", "None"),
    ("Kavya Mehta", 7.0, "Economic Analysis, Financial Modeling", "Kolkata", "Ph.D", "RBI", "CMA"),
    ("Aditya Verma", 6.2, "Investment Banking, Mergers & Acquisitions", "Mumbai", "MBA", "ICICI Bank", "CFA"),
    ("Priya Sharma", 5.8, "Risk Management, Financial Analysis", "Bangalore", "M.Com", "Axis Bank", "FRM"),
    ("Kunal Bansal", 4.5, "Wealth Management, Portfolio Strategy", "Delhi", "BBA", "HDFC Bank", "CFP"),
    ("Anjali Nair", 7.9, "Loan Processing, Credit Assessment", "Hyderabad", "B.Sc", "SBI", "CFRM"),
    ("Amit Reddy", 8.4, "Corporate Treasury, Debt Management", "Chennai", "M.Tech", "Standard Chartered", "FRM")
]
schemaBanking = StructType([
    StructField("full_name", StringType(), True),
    StructField("experience_years", FloatType(), True),
    StructField("specialization", StringType(), True),
    StructField("location", StringType(), True),
    StructField("education_level", StringType(),True),
    StructField("current_bank", StringType(),True),
    StructField("certifications", StringType(),True)
])
dfBanking = spark.createDataFrame(dataBanking, schema=schemaBanking)
dfBanking.show()
#19 Find the top 3 banks with the highest number of professionals.
df_bank=dfBanking.groupBy(col("current_bank")).agg(count(col("full_name")).alias("total_professionals"))
df_bank.orderBy(desc(col("total_professionals"))).show(3)

#20Find specializations where the total experience is above 5 years and sort them in descending order.
df_spec_exp=dfBanking.groupBy(col("specialization")).agg(sum(col("experience_years")).alias("total_exp"))
df_spec_exp.show()
df_spec_exp.filter(col("total_exp")>5).orderBy(desc(col("total_exp"))).show()

=============================================


String manipulation 
======================
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("String manipulation and number manipulation - concat").getOrCreate()
concat_data =[(("Hello"),("World"))]
columns=["subject","subject2"]
df = spark.createDataFrame(concat_data,columns)
df.show()
concat_df = df.select(concat(col("subject"),lit(" "),col("subject2")).alias("Two_columns"))
concat_df.show()
#
+-------+--------+
|subject|subject2|
+-------+--------+
|  Hello|   World|
+-------+--------+

+-----------+
|Two_columns|
+-----------+
|Hello World|
+-----------+

=====================
regex_extract(col,pattern,1)
col - column name 
pattern -- according to the pattern will extract the data 
[0-9+] -- + one or more


abc123xyz123

regex_extract(col,"[0-9]+",1]

abc123xyz123
extarct abc xyz 
regex_extract(col,"[A-Za-Z]+",1]


first word sentence 
Input = "Welcome to Spark"

regex_extract(col,"^([a-zA-z]+)",1)

extract domain 

xyz@gmail.com 
regex_extract(col,"^@([a-zA-z0-9]+)",1)


Top-level domain 

xyz@gmail.com

regex_extract(col,"//.([a-z]+)",1)

extract between _ scores 
user_active_status
regex_extract(col,"_([a-zA-z)+)_$",1)


extarct country code from code 
+91-9000202020

regex_extract(col,"//+([0-9]+)-$",1)

last 4 digits of card 
    regex_extract(col,"//-([0-9]+)$",3)
    
    
 extract month 
 2024-04-20
 regex_extract(col,"//-([0-9]+)$",1)
 
 
 extract decimal
 
 23.45
 regex_extract(col,"//.([0-9]+)$",1)
 
 
 capital word first 
 
 "hello World from Spark"
 
 regex_extract(col,"//b([A-Z]+)",1)  
 
 \w -- space 
 
 extract year from sentence
 "launch to 2019"
 regex_extract(col,"\w[0-9
============================================================================

𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
You are given a nested JSON file named sample_data.json stored in an S3 bucket at s3://your-bucket/sample_data.json. The JSON file contains details about employees, including their names, departments, and address details (nested fields).
Write a PySpark program to:
Load the JSON file into a DataFrame.
Flatten the nested structure to create a tabular format.
Write the resulting DataFrame as a Parquet file to the output path s3://your-bucket/output/.


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Load json from s3 bucket").getOrCreate()
#input_path ="s3:://your-bucket/sample_data.json"
input_json_path="/content/sample_data.json"
# Define the schema for the data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("address", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True)
])
df = spark.read.json(input_json_path,schema=schema)
flatten_df = df.select("id","name","department","address.city","address.state").withColumnRenamed("city","address.city").withColumnRenamed("state","address.state")
flatten_df.show()
#
output_path="s3://your-bucket//output"
flatten_df.write.mode("overwrite").parquet(output_path)


======================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("find total salary").getOrCreate()

salary_data =[(101,"Rajesh","IT",75000,"Bangalore"),
              (102,"Priya","HR",60000,"Mumbai"),
              (103,"Anil","IT",80000,"Hyderabad"),
              (104,"Sneha","HR",62000,"Pune"),
              (105,"Manish","Finance",90000,"Chennai"),
              (106,"Suresh","IT",78000,"Bangalore")]
columns_data = ["id","name","dept","salary","location"]
df = spark.createDataFrame(salary_data,columns_data)
df.show()
#total salary paid in each dept
grp_df = df.groupBy(col("dept")).agg(sum(col("salary")).alias("Total_salary"))
grp_df.show()

+---+------+-------+------+---------+
| id|  name|   dept|salary| location|
+---+------+-------+------+---------+
|101|Rajesh|     IT| 75000|Bangalore|
|102| Priya|     HR| 60000|   Mumbai|
|103|  Anil|     IT| 80000|Hyderabad|
|104| Sneha|     HR| 62000|     Pune|
|105|Manish|Finance| 90000|  Chennai|
|106|Suresh|     IT| 78000|Bangalore|
+---+------+-------+------+---------+

+-------+------------+
|   dept|Total_salary|
+-------+------------+
|     HR|      122000|
|     IT|      233000|
|Finance|       90000|
+-------+------------+

==============

𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧:
You are given a dataset containing sales data for different stores across various months. 
Each row contains the store name, the month, and the sales amount. 
Your task is to calculate the cumulative sales for each store, considering the monthly sales, using PySpark.

You should also:
Filter out stores with sales lower than 1000 in any month.
Calculate the total sales for each store over all months.
Sort the results by the total sales in descending order.


step1 
filter the df with >=1000

step2 
use window and then perfrom cummulative on store wise and order by month 

step 3 
group the stores and  sum on totals 

step 4

join step2 o/p and step3 o/p
 

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
# Create a SparkSession
spark = SparkSession.builder.appName("cumulative sales for each store").getOrCreate()
data = [ ("Store A", "2024-01", 800), ("Store A", "2024-02", 1200), ("Store A", "2024-03", 900), ("Store B", "2024-01", 1500), ("Store B", "2024-02", 1600), ("Store B", "2024-03", 1400), ("Store C", "2024-01", 700), ("Store C", "2024-02", 1000), ("Store C", "2024-03", 800) ] 

#Create DataFrame 
df = spark.createDataFrame(data, ["Store", "Month", "Sales"]) 
df.show()
#filter the data lower than 1000
filtered_df = df.filter(col("Sales")>=1000)
filtered_df.show()
#Window to calculate cumaltive 
window_spec = Window.partitionBy(col("store")).orderBy(col("month"))
df_cummulative = filtered_df.withColumn("cummulative_sales",sum(col("sales")).over(window_spec))
df_cummulative.show()
#calculate total sales per store
grp_df = df_cummulative.groupBy(col("store")).agg(sum(col("sales")).alias("Total_sales"))
grp_df.show()
results_df = grp_df.join(df_cummulative,on="store").select(col("store"),col("Total_sales"),col("cummulative_sales"))
results_df.orderBy(desc(col("cummulative_sales"))).show()


+-------+-----------+
|  store|Total_sales|
+-------+-----------+
|Store B|       4500|
|Store A|       1200|
|Store C|       1000|
+-------+-----------+

+-------+-----------+-----------------+
|  store|Total_sales|cummulative_sales|
+-------+-----------+-----------------+
|Store B|       4500|             4500|
|Store B|       4500|             3100|
|Store B|       4500|             1500|
|Store A|       1200|             1200|
|Store C|       1000|             1000|
+-------+-----------+-----------------+

===========


Problem Statement:
We are given a table of Uber rides that contains information about the mileage and the purpose of the business expense. 
Our task is to find the top 3 business purpose categories that generate the most miles driven by passengers using Uber for business transportation.


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize Spark session
spark = SparkSession.builder.appName("TopBusinessPurposes").getOrCreate()

# Sample data with integer and float values for miles
data = [
    ('2016-01-01 21:11', '2016-01-01 21:17', 'Business', 'Fort Pierce', 'Fort Pierce', 5.1, 'Meal/Entertain'),
    ('2016-01-02 01:25', '2016-01-02 01:37', 'Business', 'Fort Pierce', 'Fort Pierce', 5, None),
    ('2016-01-02 20:25', '2016-01-02 20:38', 'Business', 'Fort Pierce', 'Fort Pierce', 4.8, 'Errand/Supplies'),
    ('2016-01-05 17:31', '2016-01-05 17:45', 'Business', 'Fort Pierce', 'Fort Pierce', 4.7, 'Meeting'),
    ('2016-01-06 14:42', '2016-01-06 15:49', 'Business', 'Fort Pierce', 'West Palm Beach', 63.7, 'Customer Visit'),
    ('2016-01-06 17:15', '2016-01-06 17:19', 'Business', 'West Palm Beach', 'West Palm Beach', 4.3, 'Meal/Entertain'),
    ('2016-01-06 17:30', '2016-01-06 17:35', 'Business', 'West Palm Beach', 'Palm Beach', 7.1, 'Meeting')
]

# Define the schema explicitly
schema = StructType([
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True),
    StructField("category", StringType(), True),
    StructField("start", StringType(), True),
    StructField("stop", StringType(), True),
    StructField("miles", FloatType(), True),  # Ensure miles is of FloatType
    StructField("purpose", StringType(), True)
])

# Convert integer miles to float in the data manually or during DataFrame creation
data_with_float_miles = [(start_date, end_date, category, start, stop, float(miles), purpose)
                         for (start_date, end_date, category, start, stop, miles, purpose) in data]

# Create DataFrame with the explicit schema
df = spark.createDataFrame(data_with_float_miles, schema)
df.printSchema()
#filter category on business 
filter_df = df.filter(col("category")=="Business")
result_df = filter_df.groupBy(col("purpose")).agg(sum(col("miles")).alias("Total_miles"))
result_df.show()
res_df=result_df.orderBy(desc(col("Total_miles"))).limit(3)
res_df.show()
+---------------+------------------+
|        purpose|       Total_miles|
+---------------+------------------+
|           NULL|               5.0|
|Errand/Supplies| 4.800000190734863|
| Meal/Entertain| 9.400000095367432|
| Customer Visit| 63.70000076293945|
|        Meeting|11.799999713897705|
+---------------+------------------+

+--------------+------------------+
|       purpose|       Total_miles|
+--------------+------------------+
|Customer Visit| 63.70000076293945|
|       Meeting|11.799999713897705|
|Meal/Entertain| 9.400000095367432|
+--------------+------------------+



===============================

Problem Statement
We are given a table called customer_state_log containing the following columns:

cust_id: The ID of the customer.
state: The state of the session, where 1 indicates the session is active and 0 indicates the session has ended.
timestamp: The timestamp when the state change occurred.
Our task is to calculate how many hours each user was active during the day based on the state transitions.


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Customer Session Hours").getOrCreate()

# Sample data (as given in the problem)
data = [
    ('c001', 1, '07:00:00'),
    ('c001', 0, '09:30:00'),
    ('c001', 1, '12:00:00'),
    ('c001', 0, '14:30:00'),
    ('c002', 1, '08:00:00'),
    ('c002', 0, '09:30:00'),
    ('c002', 1, '11:00:00'),
    ('c002', 0, '12:30:00'),
    ('c002', 1, '15:00:00'),
    ('c002', 0, '16:30:00'),
    ('c003', 1, '09:00:00'),
    ('c003', 0, '10:30:00'),
    ('c004', 1, '10:00:00'),
    ('c004', 0, '10:30:00'),
    ('c004', 1, '14:00:00'),
    ('c004', 0, '15:30:00'),
    ('c005', 1, '10:00:00'),
    ('c005', 0, '14:30:00'),
    ('c005', 1, '15:30:00'),
    ('c005', 0, '18:30:00')
]

# Create a DataFrame
columns = ["cust_id", "state", "timestamp"]
df = spark.createDataFrame(data, columns)
df.show()
# Convert 'timestamp' to a proper timestamp type (using 'HH:MM:SS' format)
df = df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))
df.show()
df.printSchema()
#lag 
window_spec = Window.partitionBy(col("cust_id")).orderBy(col("timestamp"))
lag_df = df.withColumn("prev_timestamp",lag(col("timestamp")).over(window_spec))
lag_df.show()
#session duration calclualtion
session_duration = lag_df.filter(col("state")==0)
session_duration.show()
session_ends_df = session_duration.withColumn("session_duration_minutes",(unix_timestamp(col("timestamp"))-unix_timestamp(col("prev_timestamp")))/60)
session_ends_df.show()
results_df = session_ends_df.groupBy(col("cust_id")).agg(sum(col("session_duration_minutes")).alias("total_active_minutes"))
results_df = results_df.withColumn("Total_active_hours",col("total_active_minutes")/60)
results_df.show()


+-------+-----+-------------------+-------------------+------------------------+
|cust_id|state|          timestamp|     prev_timestamp|session_duration_minutes|
+-------+-----+-------------------+-------------------+------------------------+
|   c001|    0|2025-04-21 09:30:00|2025-04-21 07:00:00|                   150.0|
|   c001|    0|2025-04-21 14:30:00|2025-04-21 12:00:00|                   150.0|
|   c002|    0|2025-04-21 09:30:00|2025-04-21 08:00:00|                    90.0|
|   c002|    0|2025-04-21 12:30:00|2025-04-21 11:00:00|                    90.0|
|   c002|    0|2025-04-21 16:30:00|2025-04-21 15:00:00|                    90.0|
|   c003|    0|2025-04-21 10:30:00|2025-04-21 09:00:00|                    90.0|
|   c004|    0|2025-04-21 10:30:00|2025-04-21 10:00:00|                    30.0|
|   c004|    0|2025-04-21 15:30:00|2025-04-21 14:00:00|                    90.0|
|   c005|    0|2025-04-21 14:30:00|2025-04-21 10:00:00|                   270.0|
|   c005|    0|2025-04-21 18:30:00|2025-04-21 15:30:00|                   180.0|
+-------+-----+-------------------+-------------------+------------------------+

+-------+--------------------+------------------+
|cust_id|total_active_minutes|Total_active_hours|
+-------+--------------------+------------------+
|   c001|               300.0|               5.0|
|   c002|               270.0|               4.5|
|   c003|                90.0|               1.5|
|   c004|               120.0|               2.0|
|   c005|               450.0|               7.5|
+-------+--------------------+------------------+


step1 
read the data into df 

step 2 
convert the HH:MM:SS to timestamp

df.withColumn("timestamp",to_timestamp(col("timestamp"))

step3 

using lag find the previous login session is happend or not 

winod_spec = Window.partitionBy("cust_id").orderBy("time_stamp")

lag_df = df.withCOlumn("previous_time",lag("timestamp").over(window_spec))

step4 
filter on session inactive
session_ends = lag_df.filter("state"==0)

step5 
session duration calculate 

session_duration = session_ends.withCOlumn("session_active_durantion_min", (unix_timestamp("timestamp")-unix_timestamp("previous_time"))/60)
step 6 

group and sum 

result_df = session_duration.groupBy("cust_id").agg(sum("session_active_durantion_min").alias("Tiotal_min"))

step 7 
convert to hours 

result_df.withColumn("tital_hours",col("Tiotal_min")/60)
result_df.show()

====================


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("percentage variance of sales").getOrCreate()
percentage_data =[('2023-10-03', 10),('2023-10-04', 20),
('2023-10-05', 60),('2023-10-06', 50),('2023-10-07', 10)]
data_columns =["date_dt","sales"]
percentage_df = spark.createDataFrame(percentage_data,data_columns)
percentage_df.show()
percentage_df.printSchema()
#convert the date column string to date
percentage_df= percentage_df.withColumn("date_dt",to_date(col("date_dt"),"yyyy-MM-dd"))
print("after converting date string column to date")
percentage_df.printSchema()
#window function 
window_spec =Window.orderBy(col("date_dt"))
#lag function to get previous sales 
previous_sales_df = percentage_df.withColumn("previous_sales",lag(col("sales")).over(window_spec))
previous_sales_df.fillna(0,subset=["previous_sales"]).show()
#calculate percentage variance
previous_sales_df.show()
#calculate percentage variance
res_df = previous_sales_df.select(col("date_dt"),((col("sales")-col("previous_sales"))*100/col("previous_sales")).alias("percentage_variance"))
res_df.filter((col("percentage_variance") > 0) | (col("percentage_variance").isNull())).show()

+----------+-----+--------------+
|   date_dt|sales|previous_sales|
+----------+-----+--------------+
|2023-10-03|   10|          NULL|
|2023-10-04|   20|            10|
|2023-10-05|   60|            20|
|2023-10-06|   50|            60|
|2023-10-07|   10|            50|
+----------+-----+--------------+

+----------+-------------------+
|   date_dt|percentage_variance|
+----------+-------------------+
|2023-10-03|               NULL|
|2023-10-04|              100.0|
|2023-10-05|              200.0|
+----------+-------------------+

===================================================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Usage of when and otherwise").getOrCreate()
when_data = [
("Ajay", 12, 90000),
("Vijay", 8, 75000),
("Bishal", 10, 60000),
("Karthik", 5, 50000),
("Deepak", 15, 120000),
("Neha", 7, 65000)
]
when_column =["name","experience","salary"]
when_df = spark.createDataFrame(when_data,when_column)
when_df.show()
#when_df.printSchema
when_result_df = when_df.withColumn("Job Category",when((col("experience")>=10)& (col("salary")>80000),"Senior High Salary").
                                    when((col("experience")>=10) & (col("salary")<80000),"Senior Low salary").
                                    when((col("experience")<10) & (col("salary")>80000),"Junior High Salary").otherwise("Junior Low Salary"))
#
when_result_df.show()

+-------+----------+------+------------------+
|   name|experience|salary|      Job Category|
+-------+----------+------+------------------+
|   Ajay|        12| 90000|Senior High Salary|
|  Vijay|         8| 75000| Junior Low Salary|
| Bishal|        10| 60000| Senior Low salary|
|Karthik|         5| 50000| Junior Low Salary|
| Deepak|        15|120000|Senior High Salary|
|   Neha|         7| 65000| Junior Low Salary|
+-------+----------+------+------------------+


==================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Usage of when and otherwise").getOrCreate()
sample_data =[
(1, "AJAY", 28),
(2, "VIJAY", 35),
(3, "MANOJ", 22)]
sample_column = ["id","name","age"]
sample_df = spark.createDataFrame(sample_data,sample_column)
sample_df.show()
sample_df.printSchema()
sample_df.explain()
new_column_df = sample_df.withColumn("Is_adult",when(col("age")>18,"True").otherwise("False"))
new_column_df.show()
new_column_df.explain()
#
+---+-----+---+--------+
| id| name|age|Is_adult|
+---+-----+---+--------+
|  1| AJAY| 28|    True|
|  2|VIJAY| 35|    True|
|  3|MANOJ| 22|    True|
+---+-----+---+--------+

==================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Usage of when and otherwise").getOrCreate()
std_data =[
(1,85),
(2, 42),
(3, 73)]
std_column = ["student_id","score"]
student_df = spark.createDataFrame(std_data,std_column)
student_df.show()
student_df.printSchema()
student_df.explain()
pass_df = student_df.withColumn("grade",when(col("score")>=50,"Pass").otherwise("Fail"))
pass_df.show()
pass_df.explain()


+----------+-----+-----+
|student_id|score|grade|
+----------+-----+-----+
|         1|   85| Pass|
|         2|   42| Fail|
|         3|   73| Pass|
+----------+-----+-----+

=======================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Usage of when and otherwise").getOrCreate()
grade_data =[
(1,1000),
(2, 200),
(3, 5000)]
grade_column = ["transaction_id","amount"]
grade_df = spark.createDataFrame(grade_data,grade_column)
grade_df.show()
grade_df.printSchema()
grade_df.explain()
#multiple conditions 
multiple_df = grade_df.withColumn("category",when(col("amount")> 1000,"High").
                                  when(col("amount").between(500,1000),"Medium").otherwise("Low"))
multiple_df.show()
multiple_df.explain()
#

+--------------+------+--------+
|transaction_id|amount|category|
+--------------+------+--------+
|             1|  1000|  Medium|
|             2|   200|     Low|
|             3|  5000|    High|
+--------------+------+--------+

===================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Usage of when and otherwise").getOrCreate()
range_data =[
(1,30.5),
(2, 150.75),
(3, 75.25)]
range_column = ["product_id","price"]
range_df = spark.createDataFrame(range_data,range_column)
range_df.show()
range_df.printSchema()
rng_df = range_df.withColumn("Price_range",when(col("price")>100,"Expensive").
                             when(col("price").between(50,100),"Moderate").otherwise("Cheap"))
rng_df.show()
rng_df.explain()



== Physical Plan ==
*(1) Project [product_id#1868L, price#1869, CASE WHEN (price#1869 > 100.0) THEN Expensive WHEN ((price#1869 >= 50.0) AND (price#1869 <= 100.0)) THEN Moderate ELSE Cheap END AS Price_range#1881]
+- *(1) Scan ExistingRDD[product_id#1868L,price#1869]


+----------+------+-----------+
|product_id| price|Price_range|
+----------+------+-----------+
|         1|  30.5|      Cheap|
|         2|150.75|  Expensive|
|         3| 75.25|   Moderate|
+----------+------+-----------+

=========================


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Usage of when and otherwise").getOrCreate()
holiday_data =[
(1, "2024-07-27"),
(2, "2024-12-25"),
(3, "2024-01-01")
]
holiday_column = ["event_id","date"]
holiday_df = spark.createDataFrame(holiday_data,holiday_column)
holiday_df = holiday_df.withColumn("date",to_date(col("date"),"yyyy-MM-dd"))
holiday_df.explain()
holiday_df.printSchema()
#Is_holiday
hly_dy = holiday_df.withColumn("is_holiday",when((col("date").isin("2024-12-25")) | (col("date").isin("2024-01-01")),"True" ).otherwise("False"))

hly_dy.explain()
hly_dy.show()

== Physical Plan ==
*(1) Project [event_id#2122L, cast(gettimestamp(date#2123, yyyy-MM-dd, TimestampType, Some(Etc/UTC), false) as date) AS date#2126]
+- *(1) Scan ExistingRDD[event_id#2122L,date#2123]


root
 |-- event_id: long (nullable = true)
 |-- date: date (nullable = true)

== Physical Plan ==
*(1) Project [event_id#2122L, date#2126, CASE WHEN ((cast(date#2126 as string) = 2024-12-25) OR (cast(date#2126 as string) = 2024-01-01)) THEN True ELSE False END AS is_holiday#2129]
+- *(1) Project [event_id#2122L, cast(gettimestamp(date#2123, yyyy-MM-dd, TimestampType, Some(Etc/UTC), false) as date) AS date#2126]
   +- *(1) Scan ExistingRDD[event_id#2122L,date#2123]


+--------+----------+----------+
|event_id|      date|is_holiday|
+--------+----------+----------+
|       1|2024-07-27|     False|
|       2|2024-12-25|      True|
|       3|2024-01-01|      True|

=======================

Problem: You have a DataFrame user_data with columns: user_id, email, signup_date, and
last_login.
 Extract the domain from the email column using regex_extract.
 Convert the domain to uppercase.
 Filter out users whose email domain ends with ".org".
 Group by the extracted domain and calculate:
o The number of users per domain.
o The average days between signup_date and last_login.
 Drop duplicate user_ids before the group by

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Mutual Friends").getOrCreate()

# Create DataFrames
users_data = [(1, 'Karl'), (2, 'Hans'), (3, 'Emma'), (4, 'Emma'), (5, 'Mike')]
friends_data = [(1, 3), (1, 5), (2, 3), (2, 4)]
users_df = spark.createDataFrame(users_data, ["user_id", "user_name"])
friends_df = spark.createDataFrame(friends_data, ["user_id", "friend_id"])
users_df.explain()
friends_df.explain()
#filter the friends from both ids 
karl_friends = friends_df.filter(col("user_id")==1).select(col("friend_id"))
karl_friends.show()
karl_friends.explain()
hans_firends = friends_df.filter(col("user_id")==2).select(col("friend_id"))
hans_firends.show()
hans_firends.explain()
#find mutual freinds 
mutual_friends_df = karl_friends.join(hans_firends,"friend_id","inner")
mutual_friends_df.show()
mutual_friends_df.explain()
#get common friend 
user_df = mutual_friends_df.join(users_df,mutual_friends_df.friend_id==users_df.user_id,"inner").select(col("user_id"),col("user_name"))
user_df.explain()
user_df.show()
#

+-------+---------+
|user_id|user_name|
+-------+---------+
|      3|     Emma|
+-------+---------+

==========================
1. Find the average salary of employees in each department and list departments with an average salary greater than 60,000.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
sp_df = spark.createDataFrame(sp_data,sp_column)
sp_df.printSchema()
#average salary
avg_df = sp_df.groupBy(col("dept")).agg(avg("salary").alias("average_salary"))
avg_df.filter(col("average_salary")>60000).show()
+---------+--------------+
|     dept|average_salary|
+---------+--------------+
|Marketing|      113665.0|
+---------+--------------+
==============


List all employees who joined in the year 2021 and work in Hyderabad.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
sp_df = spark.createDataFrame(sp_data,sp_column)
sp_df.printSchema()
#convert the date column to date 
sp_df = sp_df.withColumn("doj",to_date(col("doj"),"yyyy-MM-dd"))
sp_df.printSchema()
#list emp joined in 2021
df_2021 = sp_df.filter(year(col("doj"))=="2021")
df_2021.explain()
df_2021.show()

+---+-----+---------+------+-----+----------+--------+-------------------+
| id| name|     dept|salary|bonus|       doj|location|              email|
+---+-----+---------+------+-----+----------+--------+-------------------+
|  2|Vijay|Marketing|118220| 1762|2021-10-25| Chennai|vijay61@example.com|
|  5| Ajay|       HR| 36626| 3168|2021-04-15| Chennai| ajay55@example.com|

==============
Identify employees who have a bonus greater than 10,000 but a salary less than 50,000.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
bns_df = spark.createDataFrame(sp_data,sp_column)
bns_df.printSchema()
#bonus >10000 and salary <5000
bns_df=bns_df.filter((col("bonus")>10000) & (col("salary")<50000))
bns_df.explain()
bns_df.show()

+---+----+----+------+-----+---+--------+-----+
| id|name|dept|salary|bonus|doj|location|email|
+---+----+----+------+-----+---+--------+-----+
+---+----+----+------+-----+---+--------+-----+

=====================

From the email column, extract the numeric part before "@example.com" and display it along with the name.
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
email_df = spark.createDataFrame(sp_data,sp_column)
email_df.printSchema()
#extract numeric part from email 
regex_pattern="([0-9]+)"
numeric_df = email_df.withColumn("numeric_part",regexp_extract(col("email"),regex_pattern,1))
numeric_df.select(col("name"),col("numeric_part")).show()
#


+-----+------------+
| name|numeric_part|
+-----+------------+
|Vijay|          17|
|Vijay|          61|
| Veer|           3|
| Ajay|          89|
| Ajay|          55|
+-----+------------+

==============

Find the top 2 highest paid employees from each department.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
top_df = spark.createDataFrame(sp_data,sp_column)
top_df.printSchema()
#Top2 
window_spec = Window.partitionBy(col("dept")).orderBy(col("salary").desc())
top2_df = top_df.withColumn("Top2Paid",row_number().over(window_spec))
top2_df.filter(col("Top2Paid")<=2).select(col("dept"),col("name"),col("salary")).show()


+---------+-----+------+
|     dept| name|salary|
+---------+-----+------+
|       HR| Ajay| 82382|
|       HR| Veer| 59682|
|Marketing|Vijay|118220|
|Marketing|Vijay|109110|

==============
Count how many employees exist in each location and department combination.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
cmb_df = spark.createDataFrame(sp_data,sp_column)
cmb_df.printSchema()
#count of empl by combination of location and dept 
grp_df = cmb_df.groupBy(col("location"),col("dept")).agg(count(col("name")).alias("count_of_emp"))
grp_df.show()

+---------+---------+------------+
| location|     dept|count_of_emp|
+---------+---------+------------+
|Hyderabad|Marketing|           1|
|  Chennai|Marketing|           1|
|  Chennai|       HR|           2|
|Bangalore|       HR|           1|
+---------+---------+------------+

===================
Retrieve the most recent and the oldest joining employee for each department.
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
old_new_df = spark.createDataFrame(sp_data,sp_column)
old_new_df.printSchema()
window_spec = Window.partitionBy(col("dept")).orderBy(col("doj").desc())
old_new_df = old_new_df.withColumn("rank",row_number().over(window_spec))
old_new_df.show()
grp_df = old_new_df.groupBy(col("dept")).agg(max(col("doj")),min(col("doj")))
grp_df.show()
+---+-----+---------+------+-----+----------+---------+-------------------+----+
| id| name|     dept|salary|bonus|       doj| location|              email|rank|
+---+-----+---------+------+-----+----------+---------+-------------------+----+
|  4| Ajay|       HR| 82382| 4755|2023-10-02|  Chennai| ajay89@example.com|   1|
|  5| Ajay|       HR| 36626| 3168|2021-04-15|  Chennai| ajay55@example.com|   2|
|  3| Veer|       HR| 59682|12173|2020-10-14|Bangalore|  veer3@example.com|   3|
|  1|Vijay|Marketing|109110| 9770|2023-07-26|Hyderabad|vijay17@example.com|   1|
|  2|Vijay|Marketing|118220| 1762|2021-10-25|  Chennai|vijay61@example.com|   2|
+---+-----+---------+------+-----+----------+---------+-------------------+----+

+---------+----------+----------+
|     dept|  max(doj)|  min(doj)|
+---------+----------+----------+
|       HR|2023-10-02|2020-10-14|
|Marketing|2023-07-26|2021-10-25|
+---------+----------+----------+


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
old_new_df = spark.createDataFrame(sp_data,sp_column)
old_new_df.printSchema()
window_spec = Window.partitionBy(col("dept")).orderBy(col("doj").desc())
recent_row_df = old_new_df.withColumn("recent_join",row_number().over(window_spec))
recent_row_df.show()
window_old = Window.partitionBy(col("dept")).orderBy(col("doj"))
old_df = old_new_df.withColumn("old_join",row_number().over(window_old))
old_df.show()
recent_row_df= recent_row_df.filter(col("recent_join")==1)
recent_row_df.show()
old_df = old_df.filter(col("old_join")==1)
old_df.show()
result = old_df.select("dept", "name", "doj").withColumnRenamed("name", "oldest_employee").withColumnRenamed("doj","old_date") \
    .join(recent_row_df.select("dept", "name", "doj").withColumnRenamed("name", "recent_employee").withColumnRenamed("doj","recent_date"), 
          on="dept", 
          how="inner") \
    .select("dept", "oldest_employee", "recent_employee","recent_date","old_date")

result.show()
#
#grp_df = row_df.groupBy(col("dept")).agg(max(col("doj")),min(col("doj")))
#grp_df.show()


=============

For each employee, show their salary difference from the department average.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
salary_diff_df = spark.createDataFrame(sp_data,sp_column)
old_new_df.printSchema()
window_spec = Window.partitionBy(col("dept"))
salary_df = salary_diff_df.withColumn("avg_dff",avg(col("salary")).over(window_spec))
result_df = salary_df.withColumn("salary_diff",round(col("salary")-col("avg_dff"),2))
result_df.show()

+---+-----+---------+------+-----+----------+---------+-------------------+------------------+-----------+
| id| name|     dept|salary|bonus|       doj| location|              email|           avg_dff|salary_diff|
+---+-----+---------+------+-----+----------+---------+-------------------+------------------+-----------+
|  3| Veer|       HR| 59682|12173|2020-10-14|Bangalore|  veer3@example.com|59563.333333333336|     118.67|
|  4| Ajay|       HR| 82382| 4755|2023-10-02|  Chennai| ajay89@example.com|59563.333333333336|   22818.67|
|  5| Ajay|       HR| 36626| 3168|2021-04-15|  Chennai| ajay55@example.com|59563.333333333336|  -22937.33|
|  1|Vijay|Marketing|109110| 9770|2023-07-26|Hyderabad|vijay17@example.com|          113665.0|    -4555.0|
|  2|Vijay|Marketing|118220| 1762|2021-10-25|  Chennai|vijay61@example.com|          113665.0|     4555.0|
+---+-----+---------+------+-----+----------+---------+-------------------+------------------+-----------+

===================

For each employee, show the next highest salary within their department.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
window_spec = Window.partitionBy(col("dept")).orderBy(col("salary").desc())
result_df = df.withColumn("next_highest_salary",lead(col("salary"),1).over(window_spec))
window_spec_next = Window.partitionBy(col("dept")).orderBy(col("next_highest_salary").desc())
next_df= result_df.withColumn("next_highest",row_number().over(window_spec_next))
next_df.show()
next_df.filter(col("next_highest")==1).select(col("dept"),col("name"),col("next_highest_salary")).show()

+---------+-----+-------------------+
|     dept| name|next_highest_salary|
+---------+-----+-------------------+
|       HR| Ajay|              59682|
|Marketing|Vijay|             109110|
+---------+-----+-------------------+

=================

Show how many employees from each department joined in each year.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
df= df.withColumn("doj",to_date(col("doj"),"yyyy-MM-dd"))
res_df = df.groupBy(year(col("doj"))).agg(count(col("name")))
res_df.show()

+---------+-----------+
|year(doj)|count(name)|
+---------+-----------+
|     2023|          2|
|     2021|          2|
|     2020|          1|
+---------+-----------+



==================

13. Rank employees within each department based on salary and list all who rank in top 3.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
#
window_spec = Window.partitionBy(col("dept")).orderBy(col("salary").desc())
rank3_df = df.withColumn("rank",rank().over(window_spec))
rank3_df.show()
result_df = rank3_df.filter(col("rank")==3)
result_df.show()


+---+----+----+------+-----+----------+--------+------------------+----+
| id|name|dept|salary|bonus|       doj|location|             email|rank|
+---+----+----+------+-----+----------+--------+------------------+----+
|  5|Ajay|  HR| 36626| 3168|2021-04-15| Chennai|ajay55@example.com|   3|
+---+----+----+------+-----+----------+--------+------------------+----+

============

14. Create a column to categorize employees as "Senior" if joined before 2022, otherwise "Junior".

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
jn_sr_df = df.withColumn("categorize",when(year(col("doj")) < "2022","Senior").otherwise("Junior"))
jn_sr_df.show()

+---+-----+---------+------+-----+----------+---------+-------------------+----------+
| id| name|     dept|salary|bonus|       doj| location|              email|categorize|
+---+-----+---------+------+-----+----------+---------+-------------------+----------+
|  1|Vijay|Marketing|109110| 9770|2023-07-26|Hyderabad|vijay17@example.com|    Junior|
|  2|Vijay|Marketing|118220| 1762|2021-10-25|  Chennai|vijay61@example.com|    Senior|
|  3| Veer|       HR| 59682|12173|2020-10-14|Bangalore|  veer3@example.com|    Senior|
|  4| Ajay|       HR| 82382| 4755|2023-10-02|  Chennai| ajay89@example.com|    Junior|
|  5| Ajay|       HR| 36626| 3168|2021-04-15|  Chennai| ajay55@example.com|    Senior|
+---+-----+---------+------+-----+----------+---------+-------------------+----------+

=======================
Find out how many employees have a bonus that is more than 10% of their salary.
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
bonus_df = df.filter(col("bonus")> col("salary")*0.10)
bonus_df.show()

+---+----+----+------+-----+----------+---------+-----------------+
| id|name|dept|salary|bonus|       doj| location|            email|
+---+----+----+------+-----+----------+---------+-----------------+
|  3|Veer|  HR| 59682|12173|2020-10-14|Bangalore|veer3@example.com|
+---+----+----+------+-----+----------+---------+-----------------+

===============

16. Display employees where the department name is not in upper case.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
upper_df = df.filter(col("dept")!= upper(col("dept")))
upper_df.show()


+---+-----+---------+------+-----+----------+---------+-------------------+
| id| name|     dept|salary|bonus|       doj| location|              email|
+---+-----+---------+------+-----+----------+---------+-------------------+
|  1|Vijay|Marketing|109110| 9770|2023-07-26|Hyderabad|vijay17@example.com|
|  2|Vijay|Marketing|118220| 1762|2021-10-25|  Chennai|vijay61@example.com|
+---+-----+---------+------+-----+----------+---------+-------------------+

=================

17. Identify the employee who joined first and last from each location.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
window_spec_first = Window.partitionBy(col("location")).orderBy(col("doj").desc())
first_join_df = df.withColumn("First_join",row_number().over(window_spec_first))
first_join_df.show()
window_spec_last = Window.partitionBy(col("location")).orderBy(col("doj"))
last_join_df = df.withColumn("last_join",row_number().over(window_spec_last))
last_join_df.show()
first_join_df = first_join_df.filter(col("First_join")==1)
last_join_df = last_join_df.filter(col("last_join")==1)
first_join_df.show()
last_join_df.show()
result = first_join_df.select("location", "name", "doj") \
    .withColumnRenamed("name", "first_joiner") \
    .withColumnRenamed("doj", "first_joined_on") \
    .join(
        last_join_df.select("location", "name", "doj") \
                    .withColumnRenamed("name", "last_joiner") \
                    .withColumnRenamed("doj", "last_joined_on"),
        on="location"
    )

# Show result
result.show()

+---------+------------+---------------+-----------+--------------+
| location|first_joiner|first_joined_on|last_joiner|last_joined_on|
+---------+------------+---------------+-----------+--------------+
|Bangalore|        Veer|     2020-10-14|       Veer|    2020-10-14|
|  Chennai|        Ajay|     2023-10-02|       Ajay|    2021-04-15|
|Hyderabad|       Vijay|     2023-07-26|      Vijay|    2023-07-26|
+---------+------------+---------------+-----------+--------------+


======

19. Display all departments that have more than 2 employees with salaries above 80,000.


19. Display all departments that have more than 2 employees with salaries above 80,000.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
result_df = df.filter(col("salary")>80000)
result_df.show()

+---+-----+---------+------+-----+----------+---------+-------------------+
| id| name|     dept|salary|bonus|       doj| location|              email|
+---+-----+---------+------+-----+----------+---------+-------------------+
|  1|Vijay|Marketing|109110| 9770|2023-07-26|Hyderabad|vijay17@example.com|
|  2|Vijay|Marketing|118220| 1762|2021-10-25|  Chennai|vijay61@example.com|
|  4| Ajay|       HR| 82382| 4755|2023-10-02|  Chennai| ajay89@example.com|


===========

20. List all employees who work in either IT or HR and are based in Bangalore or Pune.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
pune_bang_df = df.filter((col("dept").isin("IT","HR")) & (col("location").isin("Bangalore","Pune")))
pune_bang_df.show()

+---+----+----+------+-----+----------+---------+-----------------+
| id|name|dept|salary|bonus|       doj| location|            email|
+---+----+----+------+-----+----------+---------+-----------------+
|  3|Veer|  HR| 59682|12173|2020-10-14|Bangalore|veer3@example.com|
+---+----+----+------+-----+----------+---------+-----------------+

=========================

21. From each location, list the employee with the highest bonus.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
bonus_df = df.groupBy(col("location")).agg(max(col("bonus")))
bonus_df.show()

+---------+----------+
| location|max(bonus)|
+---------+----------+
|  Chennai|      4755|
|Hyderabad|      9770|
|Bangalore|     12173|

==============

22. For each name, compute the number of times it appears and the max salary they have.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
name_df = df.groupBy(col("name")).agg(count("name"),max(col("salary")))
name_df.show()

+-----+-----------+-----------+
| name|count(name)|max(salary)|
+-----+-----------+-----------+
|Vijay|          2|     118220|
| Ajay|          2|      82382|
| Veer|          1|      59682|
+-----+-----------+-----------+

==============

23. From the DataFrame, drop all rows where either salary or bonus is null.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.show()
df.printSchema()
drop_null = df.na.drop(subset=["bonus","salary"])
drop_null.show()

============

24 Replace null values in the bonus column with the department-wise average bonus

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.show()
df.printSchema()
dept_wise_df = df.groupBy(col("dept")).agg(avg(col("bonus")).alias("avg_bonus"))
dept_wise_df.fillna(0,subset=["avg_bonus"]).show()

+---------+-----------------+
|     dept|        avg_bonus|
+---------+-----------------+
|Marketing|           5766.0|
|       HR|6698.666666666667|
+---------+-----------------+

============
28. Find departments that do not have any employee based in Chennai.


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
chennai_df = df.filter((col("name").isNull()) & (col("location")=="Chennai"))
chennai_df.explain()
chennai_df.show()



============
29. For each employee, calculate the difference in days between today and their joining date.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("Spark coding exam by Karthik").getOrCreate()
sp_data = [
(1, "Vijay", "Marketing", 109110, 9770, "2023-07-26", "Hyderabad", "vijay17@example.com"),
(2, "Vijay", "Marketing", 118220, 1762, "2021-10-25", "Chennai", "vijay61@example.com"),
(3, "Veer", "HR", 59682, 12173, "2020-10-14", "Bangalore", "veer3@example.com"),
(4, "Ajay", "HR", 82382, 4755, "2023-10-02", "Chennai", "ajay89@example.com"),
(5, "Ajay", "HR", 36626, 3168, "2021-04-15", "Chennai", "ajay55@example.com")]
sp_column = ["id","name","dept","salary","bonus","doj","location","email"]
df = spark.createDataFrame(sp_data,sp_column)
df.printSchema()
df = df.withColumn("doj",to_date(col("doj"),"yyyy-MM-dd"))
df.printSchema()
group_df = df.withColumn("diff",datediff(current_date(),col("doj")).alias("difference"))
group_df.show()

+---+-----+---------+------+-----+----------+---------+-------------------+----+
| id| name|     dept|salary|bonus|       doj| location|              email|diff|
+---+-----+---------+------+-----+----------+---------+-------------------+----+
|  1|Vijay|Marketing|109110| 9770|2023-07-26|Hyderabad|vijay17@example.com| 637|
|  2|Vijay|Marketing|118220| 1762|2021-10-25|  Chennai|vijay61@example.com|1276|
|  3| Veer|       HR| 59682|12173|2020-10-14|Bangalore|  veer3@example.com|1652|
|  4| Ajay|       HR| 82382| 4755|2023-10-02|  Chennai| ajay89@example.com| 569|
|  5| Ajay|       HR| 36626| 3168|2021-04-15|  Chennai| ajay55@example.com|1469|
+---+-----+---------+------+-----+----------+---------+-------------------+----+

===================

Uber 
1. signUp details 
2. Transaction details 
as two datasets
Our goal is to calculate:
The average signup duration in minutes for each location.
The average transaction amount for each location.
The ratio of the average transaction amount to the average signup duration.
Sort the results by the highest ratio to identify the most profitable location.
Let’s break down how we can achieve this in PySpark.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Initialize Spark session
spark = SparkSession.builder.appName("UberProfitableLocation").getOrCreate()

# Sample Data - Creating the DataFrames for signups and transactions
data_signups = [
    (1, '2020-01-01 10:00:00', '2020-01-01 12:00:00', 101, 'New York'),
    (2, '2020-01-02 11:00:00', '2020-01-02 13:00:00', 102, 'Los Angeles'),
    (3, '2020-01-03 10:00:00', '2020-01-03 14:00:00', 103, 'Chicago'),
    (4, '2020-01-04 09:00:00', '2020-01-04 10:30:00', 101, 'San Francisco'),
    (5, '2020-01-05 08:00:00', '2020-01-05 11:00:00', 102, 'New York')
]

data_transactions = [
    (1, 1, '2020-01-01 10:30:00', 50.00),
    (2, 1, '2020-01-01 11:00:00', 30.00),
    (3, 2, '2020-01-02 11:30:00', 100.00),
    (4, 2, '2020-01-02 12:00:00', 75.00),
    (5, 3, '2020-01-03 10:30:00', 120.00),
    (6, 4, '2020-01-04 09:15:00', 80.00),
    (7, 5, '2020-01-05 08:30:00', 90.00)
]
# Define columns for signups DataFrame
columns_signups = ["signup_id", "signup_start_date", "signup_stop_date", "plan_id", "location"]
signups_df = spark.createDataFrame(data_signups, columns_signups)

# Define columns for transactions DataFrame
columns_transactions = ["transaction_id", "signup_id", "transaction_start_date", "amt"]
transactions_df = spark.createDataFrame(data_transactions, columns_transactions)
# get the signup diff in mintues to get
#Calculate signup duration in minutes for each signup
signup_df = signups_df.withColumn("signup_duration_minutes",(unix_timestamp(col("signup_stop_date"))-unix_timestamp(col("signup_start_date")))/60)
signup_df.explain()
signup_df.show()
#The average transaction amount for each location.
transaction_avg_df = transactions_df.groupBy("signup_id").agg(avg(col("amt")).alias("avg_tansac_amt"))
transaction_avg_df.show()
#Join both df to 
joined_df = signup_df.join(transaction_avg_df,"signup_id","inner")
joined_df.show()
# 4. Group by location and calculate average duration, average transaction amount, and ratio
results_df = joined_df.groupBy(col("location")).agg(avg(col("avg_tansac_amt")).alias("avg_tansac_amt"),avg(col("signup_duration_minutes")).alias("avg_duration"))
results_df.show()
results_df = results_df.withColumn(
    "ratio", 
    when(col("avg_duration") != 0, (col("avg_tansac_amt") / col("avg_duration"))).otherwise(0)
)
results_df.orderBy(col("ratio").desc()).show()
== Physical Plan ==
*(1) Project [signup_id#1181L, signup_start_date#1182, signup_stop_date#1183, plan_id#1184L, location#1185, (cast((unix_timestamp(signup_stop_date#1183, yyyy-MM-dd HH:mm:ss, Some(Etc/UTC), false) - unix_timestamp(signup_start_date#1182, yyyy-MM-dd HH:mm:ss, Some(Etc/UTC), false)) as double) / 60.0) AS signup_duration_minutes#1199]
+- *(1) Scan ExistingRDD[signup_id#1181L,signup_start_date#1182,signup_stop_date#1183,plan_id#1184L,location#1185]


+---------+-------------------+-------------------+-------+-------------+-----------------------+
|signup_id|  signup_start_date|   signup_stop_date|plan_id|     location|signup_duration_minutes|
+---------+-------------------+-------------------+-------+-------------+-----------------------+
|        1|2020-01-01 10:00:00|2020-01-01 12:00:00|    101|     New York|                  120.0|
|        2|2020-01-02 11:00:00|2020-01-02 13:00:00|    102|  Los Angeles|                  120.0|
|        3|2020-01-03 10:00:00|2020-01-03 14:00:00|    103|      Chicago|                  240.0|
|        4|2020-01-04 09:00:00|2020-01-04 10:30:00|    101|San Francisco|                   90.0|
|        5|2020-01-05 08:00:00|2020-01-05 11:00:00|    102|     New York|                  180.0|
+---------+-------------------+-------------------+-------+-------------+-----------------------+

+---------+--------------+
|signup_id|avg_tansac_amt|
+---------+--------------+
|        1|          40.0|
|        2|          87.5|
|        5|          90.0|
|        3|         120.0|
|        4|          80.0|
+---------+--------------+

+---------+-------------------+-------------------+-------+-------------+-----------------------+--------------+
|signup_id|  signup_start_date|   signup_stop_date|plan_id|     location|signup_duration_minutes|avg_tansac_amt|
+---------+-------------------+-------------------+-------+-------------+-----------------------+--------------+
|        1|2020-01-01 10:00:00|2020-01-01 12:00:00|    101|     New York|                  120.0|          40.0|
|        2|2020-01-02 11:00:00|2020-01-02 13:00:00|    102|  Los Angeles|                  120.0|          87.5|
|        5|2020-01-05 08:00:00|2020-01-05 11:00:00|    102|     New York|                  180.0|          90.0|
|        3|2020-01-03 10:00:00|2020-01-03 14:00:00|    103|      Chicago|                  240.0|         120.0|
|        4|2020-01-04 09:00:00|2020-01-04 10:30:00|    101|San Francisco|                   90.0|          80.0|
+---------+-------------------+-------------------+-------+-------------+-----------------------+--------------+

+-------------+--------------+------------+
|     location|avg_tansac_amt|avg_duration|
+-------------+--------------+------------+
|  Los Angeles|          87.5|       120.0|
|San Francisco|          80.0|        90.0|
|      Chicago|         120.0|       240.0|
|     New York|          65.0|       150.0|
+-------------+--------------+------------+

+-------------+--------------+------------+-------------------+
|     location|avg_tansac_amt|avg_duration|              ratio|
+-------------+--------------+------------+-------------------+
|San Francisco|          80.0|        90.0| 0.8888888888888888|
|  Los Angeles|          87.5|       120.0| 0.7291666666666666|
|      Chicago|         120.0|       240.0|                0.5|
|     New York|          65.0|       150.0|0.43333333333333335|
+-------------+--------------+------------+-------------------+

======================
We need to Get non repeated employee details.


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Non -repeated employee").getOrCreate()
data = [
    (100, 'IT', 100, '2024-05-12'),
    (200, 'IT', 100, '2024-06-12'),
    (100, 'FIN', 400, '2024-07-12'),
    (300, 'FIN', 500, '2024-07-12'),
    (300, 'FIN', 1543, '2024-07-12'),
    (300, 'FIN', 1500, '2024-07-12')
]
columns = ["empid", "dept", "salary", "date"]
df = spark.createDataFrame(data, columns)
df.show()
#get the count of empId how many times repeated
repeated_df = df.groupBy(col("empid")).agg(count(col("empid")).alias("count"))
repeated_df.show()
non_repeated_df = repeated_df.filter(col("count")==1)
non_repeated_df.show()
final_df = df.join(non_repeated_df,"empid","inner")
final_df.select(col("empid"),col("dept"),col("salary"),col("date")).show()
#

+-----+----+------+----------+
|empid|dept|salary|      date|
+-----+----+------+----------+
|  200|  IT|   100|2024-06-12|
+-----+----+------+----------+

=========================
extract domain using regex_extract

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("regex - pattern ").getOrCreate()
users_list =[
    ("U001","john.doe@gmail.com","2024-01-01","2024-03-01"),
    ("U002","jane.smith@outlook.com","2024-02-15","2024-03-10"),
    ("U003","alice.jones@yahoo.org","2024-03-01","2024-03-20")
]
users_data =["id","email","start_date","end_date"]
df = spark.createDataFrame(users_list,users_data)

df.printSchema()
#using regext_extract domain 
domain_df = df.withColumn("Domain",regexp_extract(col("email"),"@([A-Za-z0-9]+)",1))
domain_df.explain()
domain_df.show()
+----+--------------------+----------+----------+-------+
|  id|               email|start_date|  end_date| Domain|
+----+--------------------+----------+----------+-------+
|U001|  john.doe@gmail.com|2024-01-01|2024-03-01|  gmail|
|U002|jane.smith@outloo...|2024-02-15|2024-03-10|outlook|
|U003|alice.jones@yahoo...|2024-03-01|2024-03-20|  yahoo|
+----+--------------------+----------+----------+-------+
===========================

Upper to domain 

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("regex - pattern ").getOrCreate()
users_list =[
    ("U001","john.doe@gmail.com","2024-01-01","2024-03-01"),
    ("U002","jane.smith@outlook.com","2024-02-15","2024-03-10"),
    ("U003","alice.jones@yahoo.org","2024-03-01","2024-03-20")
]
users_data =["id","email","start_date","end_date"]
df = spark.createDataFrame(users_list,users_data)

df.printSchema()
#using regext_extract domain 
domain_df = df.withColumn("Domain",regexp_extract(col("email"),"@([A-Za-z0-9]+)",1))
domain_df.explain()
domain_df.show()
#domain ti=o upper case 
upper_df = domain_df.select(upper(col("domain")))
upper_df.show()

+-------------+
|upper(domain)|
+-------------+
|        GMAIL|
|      OUTLOOK|
|        YAHOO|
+-------------+

================

ends with org 

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("regex - pattern ").getOrCreate()
users_list =[
    ("U001","john.doe@gmail.com","2024-01-01","2024-03-01"),
    ("U002","jane.smith@outlook.com","2024-02-15","2024-03-10"),
    ("U003","alice.jones@yahoo.org","2024-03-01","2024-03-20")
]
users_data =["id","email","start_date","end_date"]
df = spark.createDataFrame(users_list,users_data)

df.printSchema()
#using regext_extract domain 
domain_df = df.withColumn("Domain",regexp_extract(col("email"),"@([A-Za-z0-9]+)",1))
domain_df.explain()
domain_df.show()
#domain ti=o upper case 
upper_df = domain_df.select(upper(col("domain")))
upper_df.show()
# whose domain ends with org 
org_df= df.withColumn("ends_with_org",regexp_extract(col("email"),"\\.([a-zA-z]+)$",1))
org_df.explain()
org_df.show()
org_df = org_df.filter(col("ends_with_org")=="org")
org_df.explain()
org_df.show()
+----+--------------------+----------+----------+-------------+
|  id|               email|start_date|  end_date|ends_with_org|
+----+--------------------+----------+----------+-------------+
|U003|alice.jones@yahoo...|2024-03-01|2024-03-20|          org|
+----+--------------------+----------+----------+-------------+

=============

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("regex - pattern ").getOrCreate()
users_list =[
    ("U001","john.doe@gmail.com","2024-01-01","2024-03-01"),
    ("U002","jane.smith@outlook.com","2024-02-15","2024-03-10"),
    ("U003","alice.jones@yahoo.org","2024-03-01","2024-03-20"),
    ("U004","john@yahoo.org","2024-03-01","2024-03-20"),
    ("U005","janes@gmail.com","2024-03-01","2024-03-20"),
    ("U006","jones@outlook.com","2024-03-01","2024-03-20")
]
users_data =["id","email","start_date","end_date"]
df = spark.createDataFrame(users_list,users_data)

df.printSchema()
#using regext_extract domain 
domain_df = df.withColumn("Domain",regexp_extract(col("email"),"@([A-Za-z0-9]+)",1))
domain_df.explain()
domain_df.show()
grp_df = domain_df.groupBy(col("Domain")).agg(count(col("id")))
grp_df.show()


+----+--------------------+----------+----------+-------+
|  id|               email|start_date|  end_date| Domain|
+----+--------------------+----------+----------+-------+
|U001|  john.doe@gmail.com|2024-01-01|2024-03-01|  gmail|
|U002|jane.smith@outloo...|2024-02-15|2024-03-10|outlook|
|U003|alice.jones@yahoo...|2024-03-01|2024-03-20|  yahoo|
|U004|      john@yahoo.org|2024-03-01|2024-03-20|  yahoo|
|U005|     janes@gmail.com|2024-03-01|2024-03-20|  gmail|
|U006|   jones@outlook.com|2024-03-01|2024-03-20|outlook|
+----+--------------------+----------+----------+-------+

+-------+---------+
| Domain|count(id)|
+-------+---------+
|  gmail|        2|
|outlook|        2|
|  yahoo|        2|
+-------+---------+

==================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("regex - pattern ").getOrCreate()
users_list =[
    ("U001","john.doe@gmail.com","2024-01-01","2024-03-01"),
    ("U002","jane.smith@outlook.com","2024-02-15","2024-03-10"),
    ("U003","alice.jones@yahoo.org","2024-03-01","2024-03-20"),
    ("U004","john@yahoo.org","2024-03-01","2024-03-20"),
    ("U005","janes@gmail.com","2024-03-01","2024-03-20"),
    ("U006","jones@outlook.com","2024-03-01","2024-03-20")
]
users_data =["id","email","start_date","end_date"]
df = spark.createDataFrame(users_list,users_data)

df.printSchema()
#using regext_extract domain 
domain_df = df.withColumn("Domain",regexp_extract(col("email"),"@([A-Za-z0-9]+)",1))
domain_df.explain()
domain_df.show()
grp_df = domain_df.groupBy(col("Domain")).agg(avg(datediff(col("end_date"),col("start_date"))).alias("date_dif"))
grp_df.show()

+-------+--------+
| Domain|date_dif|
+-------+--------+
|  gmail|    39.5|
|outlook|    21.5|
|  yahoo|    19.0|
+-------+--------+

=============

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("regex - pattern ").getOrCreate()
users_list =[("+1-91-9916953395",)
]
users_data =["phone"]
df = spark.createDataFrame(users_list,users_data)

df.show()
phone_df = df.select(col("phone"),split(col("phone"),"-").getItem(0).alias("cntry_code"),
                     split(col("phone"),"-").getItem(1).alias("region_code"),split(col("phone"),"-").getItem(2).alias("number"))
phone_df.show()

+----------------+----------+-----------+----------+
|           phone|cntry_code|region_code|    number|
+----------------+----------+-----------+----------+
|+1-91-9916953395|        +1|         91|9916953395|
+----------------+----------+-----------+----------+

=====================

Problem: You have a DataFrame product_data with columns: product_id, product_code,
release_date, and category.
 Split the product_code into three separate columns: brand_code, category_code, and
serial_number.
 Convert the category column to title case using initcap.
 Filter out products where brand_code starts with "X" and serial_number ends with "99".
 Group by category_code and calculate:
o The sum of serial numbers per category.
o The count of distinct brand_codes per category.
 Drop duplicates based on product_code.
Sample Data:
product_id product_code release_date category
P001 A123-456-789 2024-04-01 electronics
P002 B234-567-891 2024-05-10 home_appliance
P003 X345-678-999 2024-06-15 furniture

===============================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("null count of the columns").getOrCreate()
data = [(1, None, 'ab'),
        (2, 10, None),
       (None, None, 'cd')]
columns = ['col1', 'col2', 'col3']
df = spark.createDataFrame(data, columns)
print(df.columns)
df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()
#df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()
#count null for each column
null_df =  df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_df.explain()
null_df.show()
#2nd approach 
df.createOrReplaceTempView("null_table")
spark.sql(""" select sum(cast(isnull(col1) as int)) as c1,sum(cast(isnull(col2) as int)) as c2,
sum(cast(isnull(col3) as int)) as c3  from null_table""").show()
# 

+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   2|   1|
+----+----+----+

+---+---+---+
| c1| c2| c3|
+---+---+---+
|  1|  2|  1|
+---+---+---+

==========================

Pyspark Interview questions recently asked in NTT Data interview.
We need to add prefix to all the columns in the given dataframe.

Lets see how we can achieve this by using Pyspark.

Mentioning the dataframe details here
data = [(101, 'IT', 1000), (102, 'HR', 900)
columns = ["empid", "dept", "salary"]
df = spark.createDataFrame(data, columns)]

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Rename column names dynamically").getOrCreate()
data = [(101, "IT", 1000), (102, "HR", 900)]
columns = ["empid", "dept", "salary"]
df = spark.createDataFrame(data, columns)
df.printSchema()
rename_column = df.withColumnRenamed("empid","pref_empid").withColumnRenamed("dept","pref_dept").withColumnRenamed("salary","pref_salary")
rename_column.printSchema()
dynamic_rename_df =[]
for columns in df.columns:
  print(columns)
  dynamic_rename_df.append("pref_"+columns)
print(dynamic_rename_df)
column_renamed = spark.createDataFrame(data,dynamic_rename_df)
column_renamed.printSchema()
#3 
df.select([col(c).alias("pref"+"_"+c) for c in df.columns]).printSchema()
#

['pref_empid', 'pref_dept', 'pref_salary']
root
 |-- pref_empid: long (nullable = true)
 |-- pref_dept: string (nullable = true)
 |-- pref_salary: long (nullable = true)


========================

𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
You are given a dataset of stock prices with the following columns:

- stock_id: Unique identifier for the stock.
- date: The date of the stock price.
- price: The price of the stock on the given date.

Your task is to calculate the 3-day rolling average of the stock price (rolling_avg) for each stock (stock_id) using a sliding window, ensuring the results are partitioned by stock_id and ordered by date.

𝐬𝐜𝐡𝐞𝐦𝐚 
data = [ ("A", "2023-01-01", 100), ("A", "2023-01-02", 105), 
("A", "2023-01-03", 110), ("A", "2023-01-04", 120), 
("B", "2023-01-01", 50), ("B", "2023-01-02", 55), 
("B", "2023-01-03", 60), ("B", "2023-01-04", 65), ] 

# Define schema columns = ["stock_id", "date", "price"] 

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("3-day rolling average").getOrCreate()
data = [ ("A", "2023-01-01", 100), ("A", "2023-01-02", 105), 
("A", "2023-01-03", 110), ("A", "2023-01-04", 120), 
("B", "2023-01-01", 50), ("B", "2023-01-02", 55), 
("B", "2023-01-03", 60), ("B", "2023-01-04", 65), ] 

columns = ["stock_id", "date", "price"] 
df = spark.createDataFrame(data,columns)
df.explain()
#3-day rolling average
window_roll = Window.orderBy(col("date")).rowsBetween(-2,0)
rolling_df = df.withColumn("3-day_rolling",avg(col("price")).over(window_roll))
rolling_df.explain()
rolling_df.rdd.getNumPartitions()
rolling_df.show()

== Physical Plan ==
*(1) Scan ExistingRDD[stock_id#93,date#94,price#95L]


== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [stock_id#93, date#94, price#95L, round(_we0#101, 2) AS 3-day_rolling#100]
   +- Window [avg(price#95L) windowspecdefinition(stock_id#93, date#94 ASC NULLS FIRST, specifiedwindowframe(RowFrame, -2, currentrow$())) AS _we0#101], [stock_id#93], [date#94 ASC NULLS FIRST]
      +- Sort [stock_id#93 ASC NULLS FIRST, date#94 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(stock_id#93, 200), ENSURE_REQUIREMENTS, [plan_id=265]
            +- Scan ExistingRDD[stock_id#93,date#94,price#95L]


+--------+----------+-----+-------------+
|stock_id|      date|price|3-day_rolling|
+--------+----------+-----+-------------+
|       A|2023-01-01|  100|        100.0|
|       A|2023-01-02|  105|        102.5|
|       A|2023-01-03|  110|        105.0|
|       A|2023-01-04|  120|       111.67|
|       B|2023-01-01|   50|         50.0|
|       B|2023-01-02|   55|         52.5|
|       B|2023-01-03|   60|         55.0|
|       B|2023-01-04|   65|         60.0|
+--------+----------+-----+-------------+

===================================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Top_run scores and two consecutive half cen").getOrCreate()
matches_data =[(1, 208, 28),(2, 105, 0),(3, 201, 75),(4, 310, 48), 
(5, 402, 52),(6, 208, 58), (7, 105, 78),(8, 402, 25),(9, 310, 0),(10, 201, 90),
(11, 208, 84),(12, 105, 102)]
matches_columns =["match_id","player_id","runs_scored"]
players_data =[(208, 'Dekock'), (105, 'Virat'),(201, 'Miller'),(310, 'Warner'), (402, 'Buttler')]
players_column = ["id","name"]
df_matches = spark.createDataFrame(matches_data,matches_columns)
df_players = spark.createDataFrame(players_data,players_column)
df_matches.explain()
df_players.explain()
join_match_player_df = df_matches.join(df_players,df_matches.player_id==df_players.id,"inner").groupBy(df_players.name.alias("player_name")).\
agg(sum(col("runs_scored")).alias("total_runs"),sum(when(col("runs_scored")>=50,1).otherwise(0)).alias("no_of_half_centuries"),sum(when(col("runs_scored")==0,1).otherwise(0)).alias("no_of_ducks"))
join_match_player_df.explain()
join_match_player_df.show()
#Get the players_name 
players_df = join_match_player_df.filter((col("no_of_half_centuries")>=2) & (col("no_of_ducks")==0)).select(col("player_name"),col("total_runs"))
players_df.explain()
players_df.orderBy(col("player_name")).show()

+-----------+----------+--------------------+-----------+
|player_name|total_runs|no_of_half_centuries|no_of_ducks|
+-----------+----------+--------------------+-----------+
|     Miller|       165|                   2|          0|
|    Buttler|        77|                   1|          0|
|     Warner|        48|                   0|          1|
|      Virat|       180|                   2|          1|
|     Dekock|       170|                   2|          0|
+-----------+----------+--------------------+-----------+

+-----------+----------+
|player_name|total_runs|
+-----------+----------+
|     Dekock|       170|
|     Miller|       165|
+-----------+----------+

=====================

You are given a large e-commerce transaction dataset stored in a partitioned format based on country. Your task is to count the distinct number of products purchased (product_id) for each customer_id in every country. The result should include the country, customer ID, and the distinct product count.

𝐬𝐜𝐡𝐞𝐦𝐚 
# Sample data data = [ ("USA", 101, "P001"), 
("USA", 101, "P002"), ("USA", 101, "P001"), 
("USA", 102, "P003"), ("USA", 102, "P003"), 
("UK", 201, "P004"), ("UK", 201, "P005"), 
("UK", 202, "P004"), ("UK", 202, "P005"), ("UK", 202, "P004") ]

 # Define schema and create DataFrame columns = ["country", "customer_id", "product_id"]
 
 
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("distinct number of products purchased (product_id) for each customer_id in every country").getOrCreate()

Sample_data = [ ("USA", 101, "P001"), 
("USA", 101, "P002"), ("USA", 101, "P001"), 
("USA", 102, "P003"), ("USA", 102, "P003"), 
("UK", 201, "P004"), ("UK", 201, "P005"), 
("UK", 202, "P004"), ("UK", 202, "P005"), ("UK", 202, "P004") ]

 # Define schema and create DataFrame 
columns = ["country", "customer_id", "product_id"]
country_df = spark.createDataFrame(Sample_data, columns)
country_df.explain(mode="formatted")     # Most detailed
country_df.explain(mode="extended")      # Includes logical + physical plans
country_df.explain(mode="simple")        # Default
#country_df.explain(mode="codegen")
group_df = country_df.groupBy(col("country"),col("customer_id")).agg(countDistinct(col("product_id")).alias("Distnct_product_count"))
group_df.explain()
group_df.show()

+-------+-----------+---------------------+
|country|customer_id|Distnct_product_count|
+-------+-----------+---------------------+
|    USA|        101|                    2|
|     UK|        202|                    2|
|     UK|        201|                    2|
|    USA|        102|                    1|
+-------+-----------+---------------------+

=========================================================

Pivot 
=====

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("pivot-").getOrCreate()
# Define data
data = [
    ("jan", "elec", 200),
    ("jan", "cloth", 300),
    ("feb", "elec", 400),
    ("feb", "cloth", 500)
]

# Define columns
columns = ["month", "category", "sales"]
pivot_df = spark.createDataFrame(data,columns)
result_df = pivot_df.groupBy(col("month")).pivot("category").agg(sum(col("sales")))
#result_df.explain(mode="formatted")
result_df.orderBy(col("month").desc()).show()



+-----+-----+----+
|month|cloth|elec|
+-----+-----+----+
|  jan|  300| 200|
|  feb|  500| 400|
+-----+-----+----+

==================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("GroupBy year-wise").getOrCreate()
# Define data
data = [
    ("2023-01-01", "New York", 100),
    ("2023-02-15", "London", 200),
    ("2023-03-10", "Paris", 300),
    ("2023-04-20", "Berlin", 400),
    ("2023-05-05", "Tokyo", 500)
]

# Define schema
# schema = StructType([
#     StructField("date",DateType(),True),
#     StructField("city",StringType(),True),
#     StructField("sales",IntegerType(),True)
# ])
schema =["order_date","city","sales"]
grop_df = spark.createDataFrame(data,schema)
grop_df.printSchema()
grop_df.show()
grop_df = grop_df.withColumn("order_date",to_date(col("order_date"),"yyyy-MM-dd"))
grop_df.printSchema()
grop_df.show()
#result_df = grop_df.groupBy(year(col("order_date")).alias("year"),month(col("order_date")).alias("month")).agg(sum(col("sales")))
result_df = grop_df.groupBy(date_format(col("order_date"),"yyyy-MM").alias("year_month")).agg(sum(col("sales")).alias("Total"))
result_df.show()

+----+-----+----------+
|year|month|sum(sales)|
+----+-----+----------+
|2023|    2|       200|
|2023|    1|       100|
|2023|    3|       300|
|2023|    4|       400|
|2023|    5|       500|
+----+-----+----------+

+----------+-----+
|year_month|Total|
+----------+-----+
|   2023-02|  200|
|   2023-01|  100|
|   2023-03|  300|
|   2023-04|  400|
|   2023-05|  500|


===========

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("TopProducts").getOrCreate()
# Define data
data = [
    ("Laptop", "order_1", 2),
    ("Phone", "order_2", 1),
    ("T-Shirt", "order_1", 3),
    ("Jeans", "order_3", 4),
    ("Chair", "order_2", 2)
]

# Define columns
columns = ["product", "order_id", "quantity"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
grp_df = df.groupBy(col("product")).agg(sum(col("quantity")).alias("total_quantity")).orderBy(col("total_quantity").desc())
grp_df.show()
+-------+--------------+
|product|total_quantity|
+-------+--------------+
|  Jeans|             4|
|T-Shirt|             3|
| Laptop|             2|
|  Chair|             2|
|  Phone|             1|


================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Average-rating per user").getOrCreate()
# Define data
data = [
    (1, 1, 4),
    (1, 2, 5),
    (2, 1, 3),
    (2, 3, 4),
    (3, 2, 5)
]

# Define columns
columns = ["user_id", "product_id", "rating"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()
average_df = df.groupBy(col("user_id")).agg(avg(col("rating")).alias("average_rating"))
average_df.orderBy(col("average_rating").desc()).show()
#

+-------+----------+------+
|user_id|product_id|rating|
+-------+----------+------+
|      1|         1|     4|
|      1|         2|     5|
|      2|         1|     3|
|      2|         3|     4|
|      3|         2|     5|
+-------+----------+------+

+-------+--------------+
|user_id|average_rating|
+-------+--------------+
|      3|           5.0|
|      1|           4.5|
|      2|           3.5|
+-------+--------------+

==================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()
# Define data
data = [
    (1, "USA", "order_1", 100),
    (1, "USA", "order_2", 200),
    (2, "UK", "order_3", 150),
    (3, "France", "order_4", 250),
    (3, "France", "order_5", 300)
]

# Define columns
columns = ["customer_id", "country", "order_id", "amount"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()
rlst_df = df.groupBy(col("country")).agg(sum(col("amount")).alias("total_amount")).orderBy(col("total_amount").desc())
rlst_df.show()

+-------+------------+
|country|total_amount|
+-------+------------+
| France|         550|
|    USA|         300|
|     UK|         150|
+-------+------------+

================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Weekend-weekday-when-otherwise").getOrCreate()

# Define data
data = [
    ("2023-04-10", 1, 100),
    ("2023-04-11", 2, 200),
    ("2023-04-12", 3, 300),
    ("2023-04-13", 1, 400),
    ("2023-04-14", 2, 500),
    ("2023-04-15", 1, 500),
    ("2023-04-16", 2, 500)
]

# Define schema
schema=["order_date","customer_id","amount"]

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.printSchema()
df.show()
df= df.withColumn("order_date",to_date(col("order_date"),"yyyy-MM-dd"))
df.printSchema()
df.show()
dy_of_week_df = df.withColumn("day_of_week",dayofweek(col("order_date")))
dy_of_week_df.show() 
week_end_df= df.withColumn("day_of_week",dayofweek(col("order_date"))).withColumn("day_type",when(dayofweek(col("order_date")).isin(1,7),"weekEnd").otherwise("weekday"))
week_end_df.show()
rest_df = week_end_df.groupBy(col("day_type")).agg(sum(col("amount")).alias("total_amount"))
rest_df.show()
# 
+----------+-----------+------+-----------+--------+
|order_date|customer_id|amount|day_of_week|day_type|
+----------+-----------+------+-----------+--------+
|2023-04-10|          1|   100|          2| weekday|
|2023-04-11|          2|   200|          3| weekday|
|2023-04-12|          3|   300|          4| weekday|
|2023-04-13|          1|   400|          5| weekday|
|2023-04-14|          2|   500|          6| weekday|
|2023-04-15|          1|   500|          7| weekEnd|
|2023-04-16|          2|   500|          1| weekEnd|
+----------+-----------+------+-----------+--------+

+--------+------------+
|day_type|total_amount|
+--------+------------+
| weekday|        1500|
| weekEnd|        1000|
+--------+------------+

===========

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("filter product starts with T").getOrCreate()

# Sample data
data = [
    ("T-Shirt", "Clothing", 20),
    ("Table", "Furniture", 150),
    ("Jeans", "Clothing", 50),
    ("Chair", "Furniture", 100)
]
# Define schema
schema=["product","category","price"]

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.printSchema()
df.show()
rst_df = df.filter(col("product").like("T%"))
rst_df.show()

+-------+---------+-----+
|product| category|price|
+-------+---------+-----+
|T-Shirt| Clothing|   20|
|  Table|Furniture|  150|
|  Jeans| Clothing|   50|
|  Chair|Furniture|  100|
+-------+---------+-----+

+-------+---------+-----+
|product| category|price|
+-------+---------+-----+
|T-Shirt| Clothing|   20|
|  Table|Furniture|  150|
+-------+---------+-----+

==================

Group customers by customer ID and calculate the total amount spent. Filter customers
who spent more than $200 in total.


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Top spenders").getOrCreate()

# Sample data
data = [
    (1, 'order_1', 100),
    (1, 'order_2', 150),
    (2, 'order_3', 250),
    (3, 'order_4', 100),
    (3, 'order_5', 120)
]

# Define schema
columns = ['user_id', 'order_id', 'amount']

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show original DataFrame
df.show()
#filter who spend more than $200
top_spend_df = df.filter(col("amount")>200)
top_spend_df.show()
grp_df = top_spend_df.groupBy(col("user_id")).agg(sum(col("amount")))
grp_df.show()

+-------+--------+------+
|user_id|order_id|amount|
+-------+--------+------+
|      1| order_1|   100|
|      1| order_2|   150|
|      2| order_3|   250|
|      3| order_4|   100|
|      3| order_5|   120|
+-------+--------+------+

+-------+--------+------+
|user_id|order_id|amount|
+-------+--------+------+
|      2| order_3|   250|
+-------+--------+------+

+-------+-----------+
|user_id|sum(amount)|
+-------+-----------+
|      2|        250|
+-------+-----------+

-===========

Group orders by order ID and create a new column named "order_status" with values
"High" for orders with an amount greater than $100, and "Low" otherwise, using withColumn


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("order_status").getOrCreate()
# Updated amounts
data = [
    ('order_1', 150),
    ('order_2', 80),
    ('order_3', 220),
    ('order_4', 50)
]
columns = ['order_id', 'amount']
# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show original DataFrame
df.show()
when_df = df.withColumn("order_status",when(col("amount")>100,"High").otherwise("Low"))
when_df.show()
grp_df = when_df.groupBy(col("order_id")).agg(sum(col("amount")).alias("Total_amount"))
grp_df.show()

+--------+------+------------+
|order_id|amount|order_status|
+--------+------+------------+
| order_1|   150|        High|
| order_2|    80|         Low|
| order_3|   220|        High|
| order_4|    50|         Low|
+--------+------+------------+

+--------+------------+
|order_id|Total_amount|
+--------+------------+
| order_2|          80|
| order_1|         150|
| order_4|          50|
| order_3|         220|
+--------+------------+


==========

 Select only "product" and "price" columns, then group by "product" and calculate the
average price.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Start Spark session
spark = SparkSession.builder.appName("ProductSales").getOrCreate()

# Sample data
product_data = [
    ("Laptop", "Electronics", 1000, 2),
    ("Phone", "Electronics", 500, 1),
    ("T-Shirt", "Clothing", 20, 3),
    ("Jeans", "Clothing", 50, 4)
]
columns = ["product", "category", "price", "quantity"]

# Create DataFrame
df = spark.createDataFrame(product_data, columns)
df.show()
filter_df = df.select(col("product"),col("price"))
filter_df.show()
grp_df = filter_df.groupBy(col("product")).agg(sum(col("price")).alias("Total_price"))
grp_df.show()

+-------+-----+
|product|price|
+-------+-----+
| Laptop| 1000|
|  Phone|  500|
|T-Shirt|   20|
|  Jeans|   50|
+-------+-----+

+-------+-----------+
|product|Total_price|
+-------+-----------+
|  Phone|        500|
| Laptop|       1000|
|T-Shirt|         20|
|  Jeans|         50|
+-------+-----------+

=============

Group orders by year and month, and calculate the total number of orders and total
amount for each month-year combination

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Start session
spark = SparkSession.builder.appName("OrderData").getOrCreate()

# Sample data
data = [
    ("2023-01-01", 1, 100),
    ("2023-02-15", 2, 200),
    ("2023-03-10", 3, 300),
    ("2023-04-20", 1, 400),
    ("2023-05-05", 2, 500)
]
columns = ["order_date", "customer_id", "amount"]

# Create DataFrame with schema (cast date properly)
df = spark.createDataFrame(data, columns).withColumn("order_date", col("order_date").cast("date"))
df.show()
df.printSchema()
year_month_df = df.groupBy(date_format(col("order_date"),"yyyy-MM").alias("year_month")).agg(count(col("customer_id")).alias("Total_orders"),sum(col("amount")).alias("Tot_amount"))
year_month_df.show()

+----------+------------+----------+
|year_month|Total_orders|Tot_amount|
+----------+------------+----------+
|   2023-02|           1|       200|
|   2023-01|           1|       100|
|   2023-03|           1|       300|
|   2023-04|           1|       400|
|   2023-05|           1|       500|
+----------+------------+----------+

===========

Group by category and find the top 2 products (by total quantity sold) within each
category


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
# Initialize Spark session
spark = SparkSession.builder.appName("TopProductsPerCategory").getOrCreate()

# Sample product data
data = [
    ("Laptop", "Electronics", 1000, 2),
    ("Phone", "Electronics", 500, 5),
    ("Headphones", "Electronics", 200, 3),
    ("T-Shirt", "Clothing", 20, 10),
    ("Jeans", "Clothing", 50, 4),
    ("Jacket", "Clothing", 80, 6),
    ("Sneakers", "Clothing", 100, 2)
]
columns = ["product", "category", "price", "quantity"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
#
df_grp= df.groupBy(col("category"),col("product")).agg(sum(col("quantity")).alias("Tot_quantity"))
df_grp.show()
window_spec = Window.partitionBy(col("category")).orderBy(col("Tot_quantity").desc())
df_rank = df_grp.withColumn("rank",rank().over(window_spec))
df_rank=df_rank.filter(col("rank")<=2)
df_rank.show()

+-----------+----------+------------+----+
|   category|   product|Tot_quantity|rank|
+-----------+----------+------------+----+
|   Clothing|   T-Shirt|          10|   1|
|   Clothing|    Jacket|           6|   2|
|Electronics|     Phone|           5|   1|
|Electronics|Headphones|           3|   2|
+-----------+----------+------------+----+
======================
 Group by product ID, calculate the average rating, weighted by the quantity sold for each
order
# Start Spark session
spark = SparkSession.builder.appName("WeightedRating").getOrCreate()

# Sample data
data = [
    (1, "order_1", 4, 2),
    (1, "order_2", 5, 1),
    (2, "order_3", 3, 4),
    (2, "order_4", 4, 3),
    (3, "order_5", 5, 1)
]
columns = ["product_id", "order_id", "rating", "quantity"]

# Create DataFrame
df = spark.createDataFrame(data, columns)


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
# Initialize Spark session
spark = SparkSession.builder.appName("TopProductsPerCategory").getOrCreate()
# Start Spark session
spark = SparkSession.builder.appName("WeightedRating").getOrCreate()

# Sample data
data = [
    (1, "order_1", 4, 2),
    (1, "order_2", 5, 1),
    (2, "order_3", 3, 4),
    (2, "order_4", 4, 3),
    (3, "order_5", 5, 1)
]
columns = ["product_id", "order_id", "rating", "quantity"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
weight_df = df.withColumn("weight_average",col("rating")*col("quantity"))
weight_df.show()
grp_df = weight_df.groupBy(col("product_id")).agg((sum(col("weight_average"))/sum(col("quantity"))).alias("weight_average"))
grp_df.show()

+----------+------------------+
|product_id|    weight_average|
+----------+------------------+
|         1| 4.333333333333333|
|         3|               5.0|
|         2|3.4285714285714284|
+----------+------------------+

===========

Group by customer ID and count the distinct number of months in which they placed
orders. Filter customers who placed orders in more than two months

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Start Spark session
spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

# Sample data
data = [
    (1, "2023-01-01"),
    (1, "2023-02-15"),
    (2, "2023-03-10"),
    (2, "2023-03-20"),
    (3, "2023-04-20"),
    (3, "2023-05-05")
]
columns = ["customer_id", "order_date"]
# Create DataFrame
df = spark.createDataFrame(data, columns).withColumn("order_date", col("order_date").cast("date"))
df.printSchema()
filter_df = df.groupBy(col("customer_id")).agg(countDistinct(date_format(col("order_date"),"yyyy-MM")).alias("distinct_dates"))
filter_df.show()
filter_df.filter(col("distinct_dates")>1).show()
#

root
 |-- customer_id: long (nullable = true)
 |-- order_date: date (nullable = true)

+-----------+--------------+
|customer_id|distinct_dates|
+-----------+--------------+
|          1|             2|
|          3|             2|
|          2|             1|
+-----------+--------------+

+-----------+--------------+
|customer_id|distinct_dates|
+-----------+--------------+
|          1|             2|
|          3|             2|
+-----------+--------------+

====================

We are given a table named famous that tracks user-follow relationships. Each record in the table represents a user_id and a follower_id, where the follower_id is following the user_id. Our task is to calculate the "famous percentage" for each user


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
# Initialize Spark session
spark = SparkSession.builder.appName("FamousPercentage").getOrCreate()

# Sample data for 'famous' table
famous_data = [
    (1, 2), (1, 3), (2, 4), (5, 1), (5, 3),
    (11, 7), (12, 8), (13, 5), (13, 10),
    (14, 12), (14, 3), (15, 14), (15, 13)
]

# Define schema for the 'famous' table
columns = ["user_id", "follower_id"]

# Create DataFrame for 'famous' table
df_famous = spark.createDataFrame(famous_data, columns)
distinct_users = df_famous.select(col("user_id")).union(df_famous.select(col("follower_id"))).distinct()
#distinct_users.explain()
distinct_users.show()
# get the total users 
total_users = distinct_users.count()
print(total_users)
# get count users per user 
grp_df=df_famous.groupBy(col("user_id")).agg(count(col("follower_id")).alias("Total_followers"))
grp_df.show()

percentage_df = grp_df.withColumn("percentage",round((col("Total_followers")/total_users)*100,2))
percentage_df.show()

+-------+---------------+----------+
|user_id|Total_followers|percentage|
+-------+---------------+----------+
|      5|              2|     15.38|
|      1|              2|     15.38|
|     11|              1|      7.69|
|      2|              1|      7.69|
|     12|              1|      7.69|
|     13|              2|     15.38|
|     14|              2|     15.38|
|     15|              2|     15.38|
+-------+---------------+----------+

==============

Second lowest salary 

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Window functions").getOrCreate()
employees_data = [
    (200, 'hasha', 40, 56000, 'Testing', 204), 
    (201, 'eswari', 34, 45000, 'Testing', 200), 
    (203, 'teju', 49, 38000, 'Data Science', 205), 
    (204, 'sanju', 29, 33000, 'Data Engineering', 210), 
    (205, 'gopesh', 36, 80000, 'Data Science', None), 
    (206, 'banu', 40, 58000, 'Data Engineering', 210),
    (207, 'rekha', 38, 45000, 'Data Analyis', 203),
    (208, 'devi', 59, 80000, 'Data Analysis', 203),
    (209, 'kumar', 43, 75000, 'Data Science', 205),
    (210, 'yogesh', 60, 33000, 'Data Engineering', None)

]

columns = [
    'id', 'name', 'age', 'salary', 'Department', 'Manager_id'
]

df = spark.createDataFrame(employees_data, columns)
df.show()
df.printSchema()
#second low- earning salary
low_sal_wind = Window.orderBy(col("salary"))
rank_df = df.withColumn("second-low",dense_rank().over(low_sal_wind))
rank_df.show()
#second low 
rank_df.filter(col("second-low")==2).show()

+---+------+---+------+----------------+----------+----------+
| id|  name|age|salary|      Department|Manager_id|second-low|
+---+------+---+------+----------------+----------+----------+
|204| sanju| 29| 33000|Data Engineering|       210|         1|
|210|yogesh| 60| 33000|Data Engineering|      NULL|         1|
|203|  teju| 49| 38000|    Data Science|       205|         2|
|201|eswari| 34| 45000|         Testing|       200|         3|
|207| rekha| 38| 45000|    Data Analyis|       203|         3|
|200| hasha| 40| 56000|         Testing|       204|         4|
|206|  banu| 40| 58000|Data Engineering|       210|         5|
|209| kumar| 43| 75000|    Data Science|       205|         6|
|205|gopesh| 36| 80000|    Data Science|      NULL|         7|
|208|  devi| 59| 80000|   Data Analysis|       203|         7|
+---+------+---+------+----------------+----------+----------+

+---+----+---+------+------------+----------+----------+
| id|name|age|salary|  Department|Manager_id|second-low|
+---+----+---+------+------------+----------+----------+
|203|teju| 49| 38000|Data Science|       205|         2|
+---+----+---+------+------------+----------+----------+

===========

Q) Return Third highest salary earning employee details using PySpark code.


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Window functions").getOrCreate()
employees_data = [
    (200, 'hasha', 40, 56000, 'Testing', 204), 
    (201, 'eswari', 34, 45000, 'Testing', 200), 
    (203, 'teju', 49, 38000, 'Data Science', 205), 
    (204, 'sanju', 29, 33000, 'Data Engineering', 210), 
    (205, 'gopesh', 36, 80000, 'Data Science', None), 
    (206, 'banu', 40, 58000, 'Data Engineering', 210),
    (207, 'rekha', 38, 45000, 'Data Analyis', 203),
    (208, 'devi', 59, 80000, 'Data Analysis', 203),
    (209, 'kumar', 43, 75000, 'Data Science', 205),
    (210, 'yogesh', 60, 33000, 'Data Engineering', None)

]

columns = [
    'id', 'name', 'age', 'salary', 'Department', 'Manager_id'
]

df = spark.createDataFrame(employees_data, columns)
df.show()
df.printSchema()
#second low- earning salary
low_sal_wind = Window.orderBy(col("salary").desc())
rank_df = df.withColumn("second-low",dense_rank().over(low_sal_wind))
rank_df.show()
#second low 
rank_df.filter(col("second-low")==3).show()

+---+------+---+------+----------------+----------+----------+
| id|  name|age|salary|      Department|Manager_id|second-low|
+---+------+---+------+----------------+----------+----------+
|205|gopesh| 36| 80000|    Data Science|      NULL|         1|
|208|  devi| 59| 80000|   Data Analysis|       203|         1|
|209| kumar| 43| 75000|    Data Science|       205|         2|
|206|  banu| 40| 58000|Data Engineering|       210|         3|
|200| hasha| 40| 56000|         Testing|       204|         4|
|201|eswari| 34| 45000|         Testing|       200|         5|
|207| rekha| 38| 45000|    Data Analyis|       203|         5|
|203|  teju| 49| 38000|    Data Science|       205|         6|
|204| sanju| 29| 33000|Data Engineering|       210|         7|
|210|yogesh| 60| 33000|Data Engineering|      NULL|         7|
+---+------+---+------+----------------+----------+----------+

+---+----+---+------+----------------+----------+----------+
| id|name|age|salary|      Department|Manager_id|second-low|
+---+----+---+------+----------------+----------+----------+
|206|banu| 40| 58000|Data Engineering|       210|         3|
+---+----+---+------+----------------+----------+----------+



===============

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("AGent-enrollment").getOrCreate()
schema = StructType([
    StructField("Agent_ID", IntegerType(), True),
    StructField("Enrollment_Date", StringType(), True),
    StructField("Unenrollment_Date", StringType(), True)
])

# Sample data
data = [
    (123, "2024-02-01", "2024-03-31"),
    (456, "2024-03-01", "2024-03-31"),
    (789, "2024-02-01", "2024-05-31")
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df.printSchema()
df=df.withColumn("Enrollment_date",to_date(col("Enrollment_date"),"yyyy-MM-dd")).withColumn("Unenrollment_Date",to_date(col("Unenrollment_Date"),"yyyy-MM-dd"))
df.printSchema()
# Generate a sequence of monthly timestamps for each enrollment period
df_months = df.withColumn(
    "month_seq",
    sequence(
        trunc(col("Enrollment_Date"), "month"),
        trunc(col("Unenrollment_Date"), "month"),
        expr("interval 1 month")
    )
).withColumn("month", explode(col("month_seq"))).drop("month_seq")
df_months.show()
grp_df = df_months.groupBy(col("month")).count().withColumnRenamed("count","No of active")
grp_df.show()

+--------+---------------+-----------------+----------+
|Agent_ID|Enrollment_date|Unenrollment_Date|     month|
+--------+---------------+-----------------+----------+
|     123|     2024-02-01|       2024-03-31|2024-02-01|
|     123|     2024-02-01|       2024-03-31|2024-03-01|
|     456|     2024-03-01|       2024-03-31|2024-03-01|
|     789|     2024-02-01|       2024-05-31|2024-02-01|
|     789|     2024-02-01|       2024-05-31|2024-03-01|
|     789|     2024-02-01|       2024-05-31|2024-04-01|
|     789|     2024-02-01|       2024-05-31|2024-05-01|
+--------+---------------+-----------------+----------+

+----------+------------+
|     month|No of active|
+----------+------------+
|2024-02-01|           2|
|2024-03-01|           3|
|2024-04-01|           1|
|2024-05-01|           1|
+----------+------------+

second approach 

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("AGent-enrollment").getOrCreate()
schema = StructType([
    StructField("Agent_ID", IntegerType(), True),
    StructField("Enrollment_Date", StringType(), True),
    StructField("Unenrollment_Date", StringType(), True)
])

# Sample data
data = [
    (123, "2024-02-01", "2024-03-31"),
    (456, "2024-03-01", "2024-03-31"),
    (789, "2024-02-01", "2024-05-31")
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df.printSchema()
df=df.withColumn("Enrollment_date",to_date(col("Enrollment_date"),"yyyy-MM-dd")).withColumn("Unenrollment_Date",to_date(col("Unenrollment_Date"),"yyyy-MM-dd"))
df.printSchema()
# Generate a sequence of monthly timestamps for each enrollment period
df_months = df.withColumn("month",explode(sequence(col("Enrollment_date"), col("Unenrollment_Date"), expr("interval 1 month"))))
df_months.show()
grp_df = df_months.groupBy(col("month")).count().withColumnRenamed("count","No of active")
grp_df.show()

+--------+---------------+-----------------+----------+
|Agent_ID|Enrollment_date|Unenrollment_Date|     month|
+--------+---------------+-----------------+----------+
|     123|     2024-02-01|       2024-03-31|2024-02-01|
|     123|     2024-02-01|       2024-03-31|2024-03-01|
|     456|     2024-03-01|       2024-03-31|2024-03-01|
|     789|     2024-02-01|       2024-05-31|2024-02-01|
|     789|     2024-02-01|       2024-05-31|2024-03-01|
|     789|     2024-02-01|       2024-05-31|2024-04-01|
|     789|     2024-02-01|       2024-05-31|2024-05-01|
+--------+---------------+-----------------+----------+

+----------+------------+
|     month|No of active|
+----------+------------+
|2024-02-01|           2|
|2024-03-01|           3|
|2024-04-01|           1|
|2024-05-01|           1|
+----------+------------+

==================

Interviewer Given This Scenario
You have been given a list of tuples, each tuple containing information on customer purchase details like customer ID, Transaction ID, and list of product IDs and their Respective Quantity purchased by customers. You can use JSON format to store products with quantity as key-value pairs in this list.

The interviewer mentioned at the start of the interview that this entire round was related to Pyspark.

Questions
Create a Data Frame and show me the exact table of how it looks.
I need to see the purchased products by customers in different records, not as in the list.
Return the customer, Transaction, and total purchased quantity by each customer as the column name as the total quantity purchased.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("EmployeeStatus").getOrCreate()
data = [
    (100, 'Cust1', [{'product_id': 'P1', 'quantity': 10}, {'product_id': 'P2', 'quantity': 20},
{'product_id': 'P3', 'quantity' : 30}]),
    (200, 'Cust2', [{'product_id': 'P1', 'quantity': 5},{'product_id': 'P1', 'quantity': 18}
,{'product_id': 'P4', 'quantity': 25}]),
(300, 'Cust3', [{'product_id': 'P2', 'quantity': 2},{'product_id': 'P4', 'quantity': 11}])
]
df = spark.createDataFrame(data, ['transaction_id', 'customer_id', 'products'])
df.show(truncate=False)
#using explode flatten the columns with new columns
explode_df = df.withColumn("product_info",explode(col("products")))
explode_df.show(truncate=False)
new_column_df = explode_df.withColumn("product_id",col("product_info.product_id")).withColumn("quantity",col("product_info.quantity"))
# now drop columns of explode and orginal products 
result_df = new_column_df.drop("product_info","products")
grp_df = result_df.groupBy(col("customer_id"),col("transaction_id")).agg(sum(col("quantity")).alias("Total_quantity"))
grp_df.show()

+-----------+--------------+--------------+
|customer_id|transaction_id|Total_quantity|
+-----------+--------------+--------------+
|      Cust1|           100|          60.0|
|      Cust2|           200|          48.0|
|      Cust3|           300|          13.0|
+-----------+--------------+--------------+

===========
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("group by ").getOrCreate()
customers= spark.read.format("csv").option("header","True").option("inferSchema","True").load("/content/customer_warehouse.txt")
customers.printSchema()
# customers.explain("formatted")
# customers.explain("extended")
grp_df = customers.groupBy(col("custID"),col("category")).agg(sum(col("amount")).alias("Total_amount"))
grp_df.show()
window_spec = Window.partitionBy(col("category")).orderBy(col("Total_amount").desc())
result_df = grp_df.withColumn("rank",dense_rank().over(window_spec))
result_df.show()
result_df.filter(col("rank")==1).show()

+------+-----------+------------+----+
|custID|   category|Total_amount|rank|
+------+-----------+------------+----+
|   100| Decoration|      480000|   1|
|   101|Electronics|       82000|   1|
|   102|    casuals|        4000|   1|
|   102|  groceries|        1500|   1|
+------+-----------+------------+----+

=============

𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧:
You have the following dataset containing sales information for different products and regions. Reshape the data using PySpark's pivot() method to calculate the total sales for each product across regions, and then optimize it further by applying specific transformations.

Task 1: Use pivot() to create a table showing the total sales for each product by region.

Task 2: Add a column calculating the percentage contribution of each region to the total sales for that product.

Task 3: Sort the data in descending order by total sales for each product.




==================================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("new column").getOrCreate()
data = [("DM-HYD-101",),("DM-CHE-102",)]
columns = ["Reg_ID"]
df = spark.createDataFrame(data,columns)
df.show()
new_column = df.withColumn("location",when(col("Reg_ID").like("DM-HYD%"),"Hyderabad").otherwise("Chennai"))
new_column.show()
# 2nd approach
new_column_df = df.withColumn("Location",when(col("Reg_ID").startswith("DM-HYD"),"Hyderabad").otherwise("Chennai"))
new_column_df.show()

+----------+
|    Reg_ID|
+----------+
|DM-HYD-101|
|DM-CHE-102|
+----------+

+----------+---------+
|    Reg_ID| location|
+----------+---------+
|DM-HYD-101|Hyderabad|
|DM-CHE-102|  Chennai|
+----------+---------+

+----------+---------+
|    Reg_ID| Location|
+----------+---------+
|DM-HYD-101|Hyderabad|
|DM-CHE-102|  Chennai|
+----------+---------+


=============
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("YoY_Growth_Calculation").getOrCreate()
# Sample Data
data = [
    (1341, 123424, 1500.60, "2019-12-31 12:00:00"),
    (1423, 123424, 1000.20, "2020-12-31 12:00:00"),
    (1623, 123424, 1246.44, "2021-12-31 12:00:00"),
    (1322, 123424, 2145.32, "2022-12-31 12:00:00"),
    (1456, 567890, 800.50, "2018-12-31 12:00:00"),
    (1534, 567890, 950.75, "2019-12-31 12:00:00"),
    (1678, 567890, 1100.90, "2020-12-31 12:00:00"),
    (1789, 567890, 1205.40, "2021-12-31 12:00:00"),
    (1890, 567890, 1400.65, "2022-12-31 12:00:00"),
    (1991, 567890, 1600.80, "2023-12-31 12:00:00")
]
#define by structType
schema = StructType([
    StructField("transactionId",IntegerType(),True),
    StructField("productId",IntegerType(),True),
    StructField("spend",FloatType(),True),
    StructField("transaction_date",StringType(),True)
])
# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
df.printSchema()
df.show()
# Extract Year from transaction_date
#df = df.withColumn("transaction_date",to_date(col("transaction_date"),"yyyy-MM-dd"))
df.printSchema()
trans_year = df.withColumn("trans_year",year(col("transaction_date")))
trans_year.show()
#get the total spend per year and productId 
group_df = trans_year.groupBy(col("trans_year"),col("productId")).agg(round(sum(col("spend")),2).alias("total_spend"))
group_df.show()
# Define Window Specification for Lag Function
window_spec = Window.partitionBy("productId").orderBy("trans_year")
#get the previous year transactions
prev_year_df = group_df.withColumn("prev_year_spend",lag(col("total_spend"),1).over(window_spec))
prev_year_df.show()
# Calculate Year-on-Year Growth Rate
df_final = prev_year_df.withColumn("YoY_growth_rate",round(((col("total_spend")-col("prev_year_spend"))/col("prev_year_spend"))*100,2))
df_final.orderBy(col("productId"),col("trans_year")).show()

#
+----------+---------+-----------+---------------+
|trans_year|productId|total_spend|prev_year_spend|
+----------+---------+-----------+---------------+
|      2019|   123424|     1500.6|           NULL|
|      2020|   123424|     1000.2|         1500.6|
|      2021|   123424|    1246.44|         1000.2|
|      2022|   123424|    2145.32|        1246.44|
|      2018|   567890|      800.5|           NULL|
|      2019|   567890|     950.75|          800.5|
|      2020|   567890|     1100.9|         950.75|
|      2021|   567890|     1205.4|         1100.9|
|      2022|   567890|    1400.65|         1205.4|
|      2023|   567890|     1600.8|        1400.65|
+----------+---------+-----------+---------------+

+----------+---------+-----------+---------------+---------------+
|trans_year|productId|total_spend|prev_year_spend|YoY_growth_rate|
+----------+---------+-----------+---------------+---------------+
|      2019|   123424|     1500.6|           NULL|           NULL|
|      2020|   123424|     1000.2|         1500.6|         -33.35|
|      2021|   123424|    1246.44|         1000.2|          24.62|
|      2022|   123424|    2145.32|        1246.44|          72.12|
|      2018|   567890|      800.5|           NULL|           NULL|
|      2019|   567890|     950.75|          800.5|          18.77|
|      2020|   567890|     1100.9|         950.75|          15.79|
|      2021|   567890|     1205.4|         1100.9|           9.49|
|      2022|   567890|    1400.65|         1205.4|           16.2|
|      2023|   567890|     1600.8|        1400.65|          14.29|
+----------+---------+-----------+---------------+---------------+

Problem Statement
For each FAANG stock, we need to:

Find the highest opening price and its corresponding month-year (Mon-YYYY).
Find the lowest opening price and its corresponding month-year.
Sort the results by ticker symbol.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("YoY_Growth_Calculation").getOrCreate()
#Initialize Spark Session
spark = SparkSession.builder.appName("FAANG Stock Analysis").getOrCreate()

# Sample dataset with 3-4 records per unique ticker
data = [
    ("2023-01-31", "AAPL", 142.28, 144.34, 142.70, 144.29),
    ("2023-02-28", "AAPL", 146.83, 149.08, 147.05, 147.41),
    ("2023-03-31", "AAPL", 161.91, 165.00, 162.44, 164.90),
    ("2023-04-30", "AAPL", 167.88, 169.85, 168.49, 169.68),
    ("2023-01-31", "AMZN", 98.32, 101.26, 98.90, 100.55),
    ("2023-02-28", "AMZN", 102.50, 105.62, 103.12, 104.45),
    ("2023-03-31", "AMZN", 109.12, 112.50, 110.32, 111.85),
    ("2023-01-31", "NFLX", 320.45, 325.50, 321.90, 324.60),
    ("2023-02-28", "NFLX", 328.20, 332.45, 329.10, 331.25),
    ("2023-03-31", "NFLX", 335.90, 340.80, 337.40, 339.75),
    ("2023-01-31", "GOOGL", 88.56, 91.22, 89.34, 90.75),
    ("2023-02-28", "GOOGL", 92.78, 95.43, 93.45, 94.80),
    ("2023-03-31", "GOOGL", 97.34, 100.12, 98.23, 99.75)
]

columns = ["date", "ticker", "open", "high", "low", "close"]
stock_prices = spark.createDataFrame(data, columns)
#stock_prices.show()
#stock_prices.printSchema()
#convert to mmm-yyy
stock_prices = stock_prices.withColumn("date",to_date(col("date"),'yyyy-MM-dd'))
#stock_prices.printSchema()
# get the mon-yyyy
get_month_year_df = stock_prices.withColumn("month_year",date_format(col("date"),"MMM-yyyy"))
#get_month_year_df.show()
# window
window_high= Window.partitionBy(col("ticker")).orderBy(col("open").desc())
window_low = Window.partitionBy(col("ticker")).orderBy(col("open").asc())
#
stock_ranks = get_month_year_df.withColumn("High_open",rank().over(window_high)).withColumn("Low_open",rank().over(window_low))
stock_ranks.show()
high_df = stock_ranks.filter(col("High_open")==1).select(col("ticker"),col("month_year"),col("open").alias("highest_open"))
low_df= stock_ranks.filter(col("Low_open")==1).select(col("ticker"),col("month_year"),col("open").alias("lowest_open"))
high_df = high_df.withColumnRenamed("month_year","highest_mth")
low_df = low_df.withColumnRenamed("month_year","lowest_mth")
#
final_df = high_df.join(low_df,"ticker")
final_df.show()

+------+-----------+------------+----------+-----------+
|ticker|highest_mth|highest_open|lowest_mth|lowest_open|
+------+-----------+------------+----------+-----------+
|  AAPL|   Apr-2023|      167.88|  Jan-2023|     142.28|
|  AMZN|   Mar-2023|      109.12|  Jan-2023|      98.32|
| GOOGL|   Mar-2023|       97.34|  Jan-2023|      88.56|
|  NFLX|   Mar-2023|       335.9|  Jan-2023|     320.45|
+------+-----------+------------+----------+-----------+


df.rdd.glom().map(len).collect()


======================

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Drop null from the column").getOrCreate()
data = [("Alice", 30), ("Bob", None), ("Catherine", 25), 
(None, 35), ("Eve", None)]

columns = ["Name", "Age"] 
df = spark.createDataFrame(data,columns)
#original values 
df.show()
# remove nulls from the age column
df_filtered = df.dropna(subset=["Age","Name"])
df_filtered.show()

+---------+----+
|     Name| Age|
+---------+----+
|    Alice|  30|
|      Bob|NULL|
|Catherine|  25|
|     NULL|  35|
|      Eve|NULL|
+---------+----+

+---------+---+
|     Name|Age|
+---------+---+
|    Alice| 30|
|Catherine| 25|
+---------+---+

=================

Use Left anti join 

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("anti left join").getOrCreate()
# Create sample DataFrames
left_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
right_data = [(1, "Engineer"), (4, "Doctor")]
left_df = spark.createDataFrame(left_data, ["id", "name"])
right_df = spark.createDataFrame(right_data, ["id", "profession"])\
#anti join 
df_lanti= left_df.join(right_df,"id","left_anti")
df_lanti.show()

+---+-------+
| id|   name|
+---+-------+
|  3|Charlie|
|  2|    Bob|


===========

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("explode vs posexplode").getOrCreate()
data = [(1, ["apple", "banana", "cherry"]), (2, ["grape", "orange"])]
df = spark.createDataFrame(data, ["id", "fruits"])
#before explode 
df.show()
df_explode= df.withColumn("explode",explode(col("fruits")))
df_explode.drop(col("fruits")).show()
#posexplode
df_posexplode = df.select(col("id"),posexplode("fruits").alias("position", "fruit"))
df_posexplode.drop(col("fruits")).show()

+---+--------------------+
| id|              fruits|
+---+--------------------+
|  1|[apple, banana, c...|
|  2|     [grape, orange]|
+---+--------------------+

+---+-------+
| id|explode|
+---+-------+
|  1|  apple|
|  1| banana|
|  1| cherry|
|  2|  grape|
|  2| orange|
+---+-------+

+---+--------+------+
| id|position| fruit|
+---+--------+------+
|  1|       0| apple|
|  1|       1|banana|
|  1|       2|cherry|
|  2|       0| grape|
|  2|       1|orange|
+---+--------+------+

====

FIll null record 
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("fill null values").getOrCreate()
# Small DataFrame (lookup table)
small_df = spark.createDataFrame([(1, "USA"), (2, "Canada")], ["id", "country"])

# Large DataFrame
large_df = spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"])
#without broadcast 
df = large_df.join(small_df,"id","left")
df.select(col("id"),col("name"),coalesce(col("country"),lit("NA"))).show()
df.fillna("NA").show()

+---+-------+---------------------+
| id|   name|coalesce(country, NA)|
+---+-------+---------------------+
|  1|  Alice|                  USA|
|  3|Charlie|                   NA|
|  2|    Bob|               Canada|
+---+-------+---------------------+

+---+-------+-------+
| id|   name|country|
+---+-------+-------+
|  1|  Alice|    USA|
|  3|Charlie|     NA|
|  2|    Bob| Canada|
+---+-------+-------+

=========
Map()

#Using Map function, it will retrun the new RDD with the same number of RDD elements

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("map_example").getOrCreate()

# Create an RDD from the list

rdd = ["Hello World","Apache Spark","Distributed computing"]
rdd_data = spark.sparkContext.parallelize(rdd)
#rdd - map which will return new rdd of number of elemenst 
mapped_rdd = rdd_data.map(lambda x: x.split(" "))
print(mapped_rdd.collect())

[['Hello', 'World'], ['Apache', 'Spark'], ['Distributed', 'computing']]

============

FlatMap()
# It will flatten the results 

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("map_example").getOrCreate()

# Create an RDD from the list

rdd = ["Hello World","Apache Spark","Distributed computing"]
rdd_data = spark.sparkContext.parallelize(rdd)
#rdd - map which will return new rdd of number of elemenst 
mapped_rdd = rdd_data.flatMap(lambda x: x.split(" "))
print(mapped_rdd.collect())

['Hello', 'World', 'Apache', 'Spark', 'Distributed', 'computing']

=================================

𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
#  You are working as a Data Engineer for a company. The sales team has provided you with 
# a dataset containing sales information. However, the data has some missing values that need to be 
# addressed before processing. You are required to perform the following tasks:
# # 1. Load the following sample dataset into a PySpark DataFrame:

# # 2. Perform the following operations:
# # a. Replace all NULL values in the Quantity column with 0.
# # b. Replace all NULL values in the Price column with the average price of the existing data.
# # c. Drop rows where the Product column is NULL.
# # d. Fill missing Sales_Date with a default value of '2025-01-01'.
# # e. Drop rows where all columns are NULL.


𝐬𝐜𝐡𝐞𝐦𝐚 
data = [ (1, "Laptop", 10, 50000, "North", "2025-01-01"), (2, "Mobile", None, 15000, "South", None), (3, "Tablet", 20, None, "West", "2025-01-03"), (4, "Desktop", 15, 30000, None, "2025-01-04"), (5, None, None, None, "East", "2025-01-05") ] columns = ["Sales_ID", "Product", "Quantity", "Price", "Region", "Sales_Date"]



===============================================

#Write a sql query to find the all dates with high temperatures compared to its  previous dates(yesterday)

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Temprature calculate").getOrCreate()
# Define the data
data = [
    (1, '2025-01-01', 10),
    (2, '2025-01-02', 12),
    (3, '2025-01-03', 13),
    (4, '2025-01-04', 11),
    (5, '2025-01-05', 12),
    (6, '2025-01-06', 9),
    (7, '2025-01-07', 10),
]
columns=['id', 'date', 'temperature']
# Create the DataFrame
df = spark.createDataFrame(data, columns)
df.printSchema()
#use window function to solve 
window_spec = Window.orderBy(col("date"))
#use lag 
temp_df = df.withColumn("prev_temp",lag(col("temperature"),1).over(window_spec))
temp_df.show()
temp_df= temp_df.filter(col("prev_temp").isNotNull())
temp_df.show()
temp_diff_df = temp_df.withColumn("temp_diff",(col("temperature")-col("prev_temp")))
temp_diff_df.show()
result_df = temp_diff_df.filter(col("temp_diff")>0).select(col("date"),"temp_diff").orderBy(col("date"))
result_df.show()
#

+---+----------+-----------+---------+---------+
| id|      date|temperature|prev_temp|temp_diff|
+---+----------+-----------+---------+---------+
|  2|2025-01-02|         12|       10|        2|
|  3|2025-01-03|         13|       12|        1|
|  4|2025-01-04|         11|       13|       -2|
|  5|2025-01-05|         12|       11|        1|
|  6|2025-01-06|          9|       12|       -3|
|  7|2025-01-07|         10|        9|        1|
+---+----------+-----------+---------+---------+

+----------+---------+
|      date|temp_diff|
+----------+---------+
|2025-01-02|        2|
|2025-01-03|        1|
|2025-01-05|        1|
|2025-01-07|        1|
+----------+---------+

================

# You are provided with a Dataframe named Products containing information about various products, 
# including their names and prices. Write a pyspark program to count number of products in each category 
# based on its price into three categories below. 
# Display the output in descending order of no of products.

# 1- "Low Price" for products with a price less than 100
# 2- "Medium Price" for products with a price between 100 and 500 (inclusive)
# 3- "High Price" for products with a price greater than 500.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("with when function").getOrCreate()
# Define schema
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", IntegerType(), True)
])

# Define data from SQL inserts
products_data = [
    (1, 'Laptop', 800),
    (2, 'Smartphone', 600),
    (3, 'Headphones', 50),
    (4, 'Tablet', 400),
    (5, 'Keyboard', 30),
    (6, 'Mouse', 15),
    (7, 'Monitor', 350),
    (8, 'Printer', 120),
    (9, 'USB Drive', 10),
    (10, 'External Hard Drive', 150),
    (11, 'Wireless Router', 80),
    (12, 'Bluetooth Speaker', 70),
    (13, 'Webcam', 45),
    (14, 'Microphone', 25),
    (15, 'Gaming Mouse', 50)
]

# Create DataFrame
products_df = spark.createDataFrame(data=products_data, schema=products_schema)

# Show the DataFrame
products_df.show()
#products_df= products_df.filter(col("price")>=100)
final_df = products_df.withColumn("category",when(col("price")<100,"Low Price").\
                                  when(col("price").between(100,500),"Medium Price").\
                                  when(col("price")>500,"High Price"))
final_df.groupBy(col("category")).agg(count(col("product_name")).alias("no_of_products")).\
orderBy(col("no_of_products").desc()).show()

+------------+--------------+
|    category|no_of_products|
+------------+--------------+
|   Low Price|             9|
|Medium Price|             4|
|  High Price|             2|
+------------+--------------+


============

𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
# You are given a dataset containing sales transactions for different products. 
# The dataset consists of columns for product_id, product_name, and sales_amount. 
# Your task is to calculate the total sales for each product 
# and filter out only the products with total sales greater than $1000.


𝐬𝐜𝐡𝐞𝐦𝐚 
data = [
 (1, "Product A", 500), (1, "Product A", 600),
 (2, "Product B", 300), (3, "Product C", 1200),
 (3, "Product C", 800), (4, "Product D", 900)
]
# Define schema and create DataFrame
columns = ["product_id", "product_name", "sales_amount"]


======================

Json 
[ {
    "id": 1,
    "name": "John",
    "age": 30,
    "address": {
      "street": "123 Main St",
      "city": "New York",
      "state": "NY"
    }
  },
  {
    "id": 2,
    "name": "Jane",
    "age": 28,
    "address": {
      "street": "456 Elm St",
      "city": "Los Angeles",
      "state": "CA"
    }
  }]


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("with semi-structured JSON").getOrCreate()
json_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True)
])
json_df = spark.read.schema(json_schema).option("multiLine",True).json("/content/file.json")
json_df.show(truncate=False)

+---+----+---+-----------------------------+
|id |name|age|address                      |
+---+----+---+-----------------------------+
|1  |John|30 |{123 Main St, New York, NY}  |
|2  |Jane|28 |{456 Elm St, Los Angeles, CA}|
+---+----+---+-----------------------------+

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
# Add the Spark XML package to the session configuration
spark = SparkSession.builder.appName("with semi-structured XML").\
config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.16.0").getOrCreate()
xml_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True)
])
# Read XML data with the defined schema into a dataframe
xml_df = spark.read.format("xml").option("rowTag",'person').schema(xml_schema).load("/content/file_xml.xml")
xml_df.show(truncate=False)

===========


#Task: 
# Write a SQL Query to find the total amount by year. Make sure to check the period_end date column to understand like when exactly the period is ending.

# I've shared my solution using hashtag#ApacheSpark and hashtag#SQL. Check out the attached image! I'm curious to see how YOU would solve this challenge.

# Would you take a different approach? Maybe using Pandas or a PySpark UDF? Let's see your innovative solutions in the comments below!

Use this below data to create table:-
create table sales (
product_id int,
period_start date,
period_end date,
average_daily_sales int
);

insert into sales values(1,'2019-01-25','2019-02-28',100),(2,'2018-12-01','2020-01-01',10),(3,'2019-12-01','2020-01-31',1);


=========================

#write a pyspark program  to find each flight source and destination.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("find source and destination of flight..").getOrCreate()
#flight details 
flight_data =  [
    (1, 'Delhi', 'Kolkata'),
    (1, 'Kolkata', 'Bangalore'),
    (2, 'Mumbai', 'Pune'),
    (2, 'Pune', 'Goa'),
    (3, 'Kolkata', 'Delhi'),
    (3, 'Delhi', 'Srinagar')
]

# Define the column names
flight_columns = ['id', 'source', 'destination']
flight_df = spark.createDataFrame(flight_data, flight_columns)
#window function 
window_spec = Window.partitionBy(col("id")).orderBy(col("id"))
lead_df = flight_df.withColumn("destination",lead(col("destination"),1).over(window_spec))
filter_df = lead_df.filter(col("destination").isNotNull())
filter_df.show()
# 
+---+-------+-----------+
| id| source|destination|
+---+-------+-----------+
|  1|  Delhi|  Bangalore|
|  2| Mumbai|        Goa|
|  3|Kolkata|   Srinagar|
+---+-------+-----------+

======================


#write a pyspark to find for each month and country, the number of transactions
#and their total amount , the number of approved transactions and their total amount.`

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *


# Create a SparkSession
spark = SparkSession.builder.appName("Month and country calculation").getOrCreate()

# Create data
data = [
    (1, 'US', 'approved', 1000, '2023-12-18'),
    (2, 'US', 'declined', 2000, '2023-12-19'),
    (3, 'US', 'approved', 2000, '2024-01-01'),
    (4, 'India', 'approved', 2000, '2023-01-07')
]

# Define the schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("date", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)
date_df = df.withColumn("date",to_date(col("date"),'yyyy-MM-dd'))
#date_df.printSchema()
date_df = date_df.withColumn("year_month",date_format(col("date"),'yyyy-MM'))
result_df = date_df.groupBy(col("year_month"),col("country")).\
agg(count(col("status")).alias("no_of_transactions"),\
    count(when(col("status")=="approved",1)).alias("no_approved_transaction"),\
    sum(col("amount")).alias("total_amount"))
result_df.orderBy(col("country")).show()

+----------+-------+------------------+-----------------------+------------+
|year_month|country|no_of_transactions|no_approved_transaction|total_amount|
+----------+-------+------------------+-----------------------+------------+
|   2023-01|  India|                 1|                      1|        2000|
|   2023-12|     US|                 2|                      1|        3000|
|   2024-01|     US|                 1|                      1|        2000|
+----------+-------+------------------+-----------------------+------------+

=================

𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
You are working with a real-time data pipeline for an e-commerce platform. Your task is to join two large DataFrames:

sales_data (large DataFrame containing sales transactions for the past year).
product_data (small DataFrame containing product details like name, category, and price).
- Due to the size of the sales_data DataFrame, it's inefficient to shuffle all the - data across the network when joining. To optimize this, you should use a -broadcast join on the small DataFrame (product_data).

You need to write a solution where you:
- Broadcast the smaller DataFrame (product_data).
- Join sales_data with product_data based on product_id.
- Ensure efficient use of resources and minimal shuffling.

𝐬𝐜𝐡𝐞𝐦𝐚 
sales_data = [ (1, 101, 5, '2025-01-01'), (2, 102, 3, '2025-01-02'), 
(3, 103, 2, '2025-01-03'), (4, 101, 1, '2025-01-04'), 
(5, 104, 4, '2025-01-05'), (6, 105, 6, '2025-01-06'), ] 

# product_data (Small DataFrame) 

product_data = [ (101, 'Laptop', 'Electronics', 1000), 
(102, 'Phone', 'Electronics', 500), 
(103, 'Headphones', 'Accessories', 150), 
(104, 'Tablet', 'Electronics', 600), 
(105, 'Smartwatch', 'Accessories', 200), ] 

=============

Q4) You are given a PySpark DataFrame with a column containing strings in the format "xxx~yyy".
 Your task is to split this column into two new columns:

🧪 Sample Input DataFrame:
input_column
xxx~yyy
abc~def
123~456

from pyspark.sql.functions import split, col

df1 = df.withColumn("firstcolumn", split(col("input_column"), "~").getItem(0)) \
 .withColumn("thirdcolumn", split(col("input_column"), "~").getItem(1))


 =====================================


 𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
Write a PySpark program to calculate the highest salary in each department and sort the output in ascending order of department name.

𝐬𝐜𝐡𝐞𝐦𝐚
data = [
 (1, "Ravi", "IT", 85000),
 (2, "Priya", "HR", 65000),
 (3, "Suresh", "IT", 92000),
 (4, "Neha", "Finance", 78000),
 (5, "Anil", "HR", 72000),
 (6, "Divya", "Finance", 88000),
 (7, "Kiran", "IT", 88000)
]
columns = ["emp_id", "emp_name", "department", "salary"]

Expected Output :
+----------+--------------+
|department|highest_salary|
+--------------+----------------------+
|Finance | 88000 |
|HR | 72000 |
|IT | 92000 |
+---------------+--------------------+


====================

Write a Pyspark program to find person_name of the last person that 
can fit on the bus without exceeding the weight limit. limit is 400;


from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Bus weiht check").getOrCreate()
# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("weight", IntegerType(), True),
    StructField("turn", IntegerType(), True)
])

# Data from insert statements
data = [
    (5, 'john', 120, 2),
    (4, 'tom', 100, 1),
    (3, 'rahul', 95, 4),
    (6, 'bhavna', 100, 5),
    (1, 'ankita', 79, 6),
    (2, 'Alex', 80, 3)
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()
#window function
window_spec = Window.orderBy(col("turn"))
#cummulative sum 
cum_sum = df.withColumn("cum_sum",sum(col("weight")).over(window_spec))
cum_sum.show()
#Filter and use row_number to get the last 
#person who sit within the limit 400
limit_df = cum_sum.filter(col("cum_sum")<=400)
limit_df.show()
#window with rank
window_rnk = Window.orderBy(col("cum_sum").desc())
rslt_df = limit_df.withColumn("rnk",row_number().over(window_rnk))
rslt_df.show()
rslt_df.filter(col("rnk")==1).select(col("name")).show()


+---+-----+------+----+-------+---+
| id| name|weight|turn|cum_sum|rnk|
+---+-----+------+----+-------+---+
|  3|rahul|    95|   4|    395|  1|
|  2| Alex|    80|   3|    300|  2|
|  5| john|   120|   2|    220|  3|
|  4|  tom|   100|   1|    100|  4|
+---+-----+------+----+-------+---+

+-----+
| name|
+-----+
|rahul|
+-----+


=========

PySpark Interview Question :

Question:

You are given a DataFrame containing customer information collected from multiple branches of a national bank in India. 
Due to manual entries and system integration, the dataset contains exact duplicates as well as partial duplicates 
(where some fields like branch_name or email may vary).

1. Remove completely duplicate rows (i.e., rows where all columns match exactly).
2. Then remove partially duplicate records where customer is considered same if their customer_name, dob, and pan_number match, keeping the most recent entry based on the updated_at column.
3. Finally, return the cleaned DataFrame sorted by state, city, and customer_name.


 Sample Data :
data = [
 ("Rajesh Kumar", "1990-05-14", "ABCDE1234F", "Delhi", "New Delhi", "Connaught Place", "rajesh.k@gmail.com", "2024-01-01"),
 ("Rajesh Kumar", "1990-05-14", "ABCDE1234F", "Delhi", "New Delhi", "Karol Bagh", "rajesh.kumar@outlook.com", "2024-02-01"),
 ("Rajesh Kumar", "1990-05-14", "ABCDE1234F", "Delhi", "New Delhi", "Karol Bagh", "rajesh.kumar@outlook.com", "2024-02-01"), # Exact Duplicate
 ("Priya Sharma", "1992-11-23", "PQRSX5678K", "Maharashtra", "Mumbai", "Andheri", "priya.sharma@gmail.com", "2023-12-15"),
 ("Amit Jain", "1985-03-02", "LMNOP3456Z", "Rajasthan", "Jaipur", "Malviya Nagar", "amit.jain@gmail.com", "2024-03-10"),
 ("Amit Jain", "1985-03-02", "LMNOP3456Z", "Rajasthan", "Jaipur", "Malviya Nagar", "ajain@yahoo.com", "2024-01-01"),
 ("Sneha Reddy", "1993-07-19", "GHJKL7890Y", "Telangana", "Hyderabad", "Banjara Hills", "sneha.reddy@gmail.com", "2024-04-05"),
 ("Sneha Reddy", "1993-07-19", "GHJKL7890Y", "Telangana", "Hyderabad", "Banjara Hills", "sneha.reddy@gmail.com", "2024-04-05"), # Exact Duplicate
]

columns = ["customer_name", "dob", "pan_number", "state", "city", "branch_name", "email", "updated_at"]

 
Expected Output :
+-------------+----------+------------+------------+---------+--------------+
|customer_name|dob    |pan_number |state    |city   |branch_name  |email           |updated_at |
+-------------+----------+------------+------------+---------+--------------+
|Amit Jain  |1985-03-02|LMNOP3456Z |Rajasthan  |Jaipur  |Malviya Nagar |amit.jain@gmail.com |2024-03-10 |
|Rajesh Kumar |1990-05-14|ABCDE1234F |Delhi    |New Delhi|Karol Bagh  |rajesh.kumar@outlook.com |2024-02-01 |
|Priya Sharma |1992-11-23|PQRSX5678K |Maharashtra |Mumbai  |Andheri    |priya.sharma@gmail.com |2023-12-15 |
|Sneha Reddy |1993-07-19|GHJKL7890Y |Telangana  |Hyderabad|Banjara Hills |sneha.reddy@gmail.com |2024-04-05 |
+-------------+----------+------------+------------+---------+--------------+


=============

# Namastekart, an e-commerce company, has observed a notable surge in return orders recently. 
# They suspect that a specific group of customers may be responsible for a significant portion of these returns. 
# To address this issue, their initial goal is to identify customers who have returned more than 50% of their orders. 
# This way, they can proactively reach out to these customers to gather feedback.

# Write an pyspark to find list of customers along with their return percent (Round to 2 decimal places), 
# display the output in ascending order of customer name.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName(" percentage calculation for the return orders").getOrCreate()
# Define schema with order_date as StringType
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("sales", IntegerType(), True)
])

# Data
orders_data = [
    (1, '2023-01-01', 'Alexa', 100),
    (2, '2023-01-02', 'Alexa', 200),
    (3, '2023-01-03', 'Alexa', 300),
    (4, '2023-01-03', 'Alexa', 400),
    (5, '2023-01-01', 'Ramesh', 500),
    (6, '2023-01-02', 'Ramesh', 600),
    (7, '2023-01-03', 'Ramesh', 700),
    (8, '2023-01-03', 'Neha', 800),
    (9, '2023-01-03', 'Ankit', 800),
    (10, '2023-01-04', 'Ankit', 900)
]

# Create DataFrame
orders_df = spark.createDataFrame(orders_data, schema=orders_schema)
orders_df.show()

# Define schema with return_date as StringType
returns_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("return_date", StringType(), True)
])

# Data
returns_data = [
    (1, '2023-01-02'),
    (2, '2023-01-04'),
    (3, '2023-01-05'),
    (7, '2023-01-06'),
    (9, '2023-01-06'),
    (10, '2023-01-07')
]

# Create DataFrame
returns_df = spark.createDataFrame(returns_data, schema=returns_schema)
returns_df.show()
orders_df = orders_df.withColumn("order_date",to_date(col("order_date"),'yyyy-MM-dd'))
returns_df = returns_df.withColumn("return_date",to_date(col("return_date"),'yyyy-MM-dd'))
orders_df.printSchema()
returns_df.printSchema()
join_df = orders_df.join(returns_df,"order_id","left")
join_df.show()
#Window to get the count 
windowOrders = Window.partitionBy(col("customer_name"))
#Window to get no of return dates
windowReturns = Window.partitionBy(col("customer_name"))
#
res_df = join_df.withColumn("no_of_orders",count(col("order_id")).over(windowOrders)).\
withColumn("no_of_returns",count(col("return_date")).over(windowReturns))
res_df.show()
final_df = res_df.groupBy(col("customer_name")).\
agg(avg(round((col("no_of_returns")/col("no_of_orders"))*100,2)).alias("return_ratio"))
final_df.show()
final_df.filter(col("return_ratio")>50).show()

+-------------+------------+
|customer_name|return_ratio|
+-------------+------------+
|        Alexa|        75.0|
|        Ankit|       100.0|
+-------------+------------+

===================

# You're working for a large financial institution that provides various types of loans to customers. 
# Your task is to analyze loan repayment data to assess credit risk and improve risk management strategies.

# Write an pyspark program to create 2 flags for each loan as per below rules. Display loan id, loan amount , 
# due date and the 2 flags.
# 1- fully_paid_flag: 1 if the loan was fully repaid irrespective of payment date else it should be 0.
# 2- on_time_flag : 1 if the loan was fully repaid on or before due date else 0.

from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
# Create a SparkSession
spark = SparkSession.builder.appName("credit risk management ").getOrCreate()
#define data for loans 
loans_data = [
    (1, 1, 5000, '2023-01-15'),
    (2, 2, 8000, '2023-02-20'),
    (3, 3, 10000,'2023-03-10'),
    (4, 4, 6000, '2023-04-05'),
    (5, 5, 7000, '2023-05-01')
]
#define schema for loans
loans_schema = StructType(
    [StructField("loan_id",IntegerType(),True),
    StructField("customer_id",IntegerType(),True),
    StructField("loan_amount",IntegerType(),True),
    StructField("due_date",StringType(),True)
    ])
#create dataframe for loans
loans_df = spark.createDataFrame(loans_data,loans_schema)
loans_df.show()
#define data for payments
payments_data = [
    (1, 1, '2023-01-10', 2000),
    (2, 1, '2023-02-10', 1500),
    (3, 2, '2023-02-20', 8000),
    (4, 3, '2023-04-20', 5000),
    (5, 4, '2023-03-15', 2000),
    (6, 4, '2023-04-02',4000),
    (7, 5, '2023-04-02',4000),
    (8, 5, '2023-05-02',3000)
]

payments_schema = StructType([
    StructField("payment_id", IntegerType(), True),
    StructField("loan_id", IntegerType(), True),
    StructField("payment_date", StringType(), True),
    StructField("amount_paid", IntegerType(), True)
    ])
#create dataframe for payments
payments_df = spark.createDataFrame(payments_data,payments_schema)
payments_df.show()
#get the cummulative sum of the amountPaid from the payments df for each loan_id 
window_spec = Window.partitionBy(col("loan_id"))
join_df = loans_df.join(payments_df,"loan_id","inner").\
withColumn("Total_amount_paid",sum(payments_df.amount_paid).over(window_spec)).\
select(loans_df.loan_id,loans_df.loan_amount,loans_df.due_date,\
    payments_df.payment_date,payments_df.amount_paid,col("Total_amount_paid"))
join_df.show()
#row_number
window_row_spec = Window.partitionBy(col("loan_id")).\
orderBy(col("Total_amount_paid"),col("payment_date").desc())
cte2_df = join_df.withColumn("row_number",row_number().over(window_row_spec))
cte2_df.show()
filter_df = cte2_df.filter(col("row_number")==1)
filter_df.show()
final_df = 
filter_df.withColumn("fully_paid_flag",
    when((col("Total_amount_paid")==col("loan_amount")) | (col("payment_date") <= col("due_date")),1).\
    otherwise(0)).withColumn("on_time_flag",
    when((col("loan_amount")==col("Total_amount_paid")) & (col("payment_date")<=col("due_date")),1).otherwise(0))
final_df.select(col("loan_id"),col("loan_amount"),col("due_date"),col("fully_paid_flag"),col("on_time_flag")).show()

+-------+-----------+----------+---------------+------------+
|loan_id|loan_amount|  due_date|fully_paid_flag|on_time_flag|
+-------+-----------+----------+---------------+------------+
|      1|       5000|2023-01-15|              0|           0|
|      2|       8000|2023-02-20|              1|           1|
|      3|      10000|2023-03-10|              0|           0|
|      4|       6000|2023-04-05|              1|           1|
|      5|       7000|2023-05-01|              1|           0|
+-------+-----------+----------+---------------+------------+

+-------+-----------+-----------+----------+
|loan_id|customer_id|loan_amount|  due_date|
+-------+-----------+-----------+----------+
|      1|          1|       5000|2023-01-15|
|      2|          2|       8000|2023-02-20|
|      3|          3|      10000|2023-03-10|
|      4|          4|       6000|2023-04-05|
|      5|          5|       7000|2023-05-01|
+-------+-----------+-----------+----------+

+----------+-------+------------+-----------+
|payment_id|loan_id|payment_date|amount_paid|
+----------+-------+------------+-----------+
|         1|      1|  2023-01-10|       2000|
|         2|      1|  2023-02-10|       1500|
|         3|      2|  2023-02-20|       8000|
|         4|      3|  2023-04-20|       5000|
|         5|      4|  2023-03-15|       2000|
|         6|      4|  2023-04-02|       4000|
|         7|      5|  2023-04-02|       4000|
|         8|      5|  2023-05-02|       3000|
+----------+-------+------------+-----------+

+-------+-----------+----------+------------+-----------+-----------------+
|loan_id|loan_amount|  due_date|payment_date|amount_paid|Total_amount_paid|
+-------+-----------+----------+------------+-----------+-----------------+
|      1|       5000|2023-01-15|  2023-01-10|       2000|             3500|
|      1|       5000|2023-01-15|  2023-02-10|       1500|             3500|
|      2|       8000|2023-02-20|  2023-02-20|       8000|             8000|
|      3|      10000|2023-03-10|  2023-04-20|       5000|             5000|
|      4|       6000|2023-04-05|  2023-03-15|       2000|             6000|
|      4|       6000|2023-04-05|  2023-04-02|       4000|             6000|
|      5|       7000|2023-05-01|  2023-04-02|       4000|             7000|
|      5|       7000|2023-05-01|  2023-05-02|       3000|             7000|
+-------+-----------+----------+------------+-----------+-----------------+

+-------+-----------+----------+------------+-----------+-----------------+----------+
|loan_id|loan_amount|  due_date|payment_date|amount_paid|Total_amount_paid|row_number|
+-------+-----------+----------+------------+-----------+-----------------+----------+
|      1|       5000|2023-01-15|  2023-02-10|       1500|             3500|         1|
|      1|       5000|2023-01-15|  2023-01-10|       2000|             3500|         2|
|      2|       8000|2023-02-20|  2023-02-20|       8000|             8000|         1|
|      3|      10000|2023-03-10|  2023-04-20|       5000|             5000|         1|
|      4|       6000|2023-04-05|  2023-04-02|       4000|             6000|         1|
|      4|       6000|2023-04-05|  2023-03-15|       2000|             6000|         2|
|      5|       7000|2023-05-01|  2023-05-02|       3000|             7000|         1|
|      5|       7000|2023-05-01|  2023-04-02|       4000|             7000|         2|
+-------+-----------+----------+------------+-----------+-----------------+----------+

+-------+-----------+----------+------------+-----------+-----------------+----------+
|loan_id|loan_amount|  due_date|payment_date|amount_paid|Total_amount_paid|row_number|
+-------+-----------+----------+------------+-----------+-----------------+----------+
|      1|       5000|2023-01-15|  2023-02-10|       1500|             3500|         1|
|      2|       8000|2023-02-20|  2023-02-20|       8000|             8000|         1|
|      3|      10000|2023-03-10|  2023-04-20|       5000|             5000|         1|
|      4|       6000|2023-04-05|  2023-04-02|       4000|             6000|         1|
|      5|       7000|2023-05-01|  2023-05-02|       3000|             7000|         1|
+-------+-----------+----------+------------+-----------+-----------------+----------+

+-------+-----------+----------+---------------+------------+
|loan_id|loan_amount|  due_date|fully_paid_flag|on_time_flag|
+-------+-----------+----------+---------------+------------+
|      1|       5000|2023-01-15|              0|           0|
|      2|       8000|2023-02-20|              1|           1|
|      3|      10000|2023-03-10|              0|           0|
|      4|       6000|2023-04-05|              1|           1|
|      5|       7000|2023-05-01|              1|           0|
+-------+-----------+----------+---------------+------------+
