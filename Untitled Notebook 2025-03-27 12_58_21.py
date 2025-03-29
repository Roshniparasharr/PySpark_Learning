# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

dbutils.fs.ls("dbfs:/Volumes/workspace/default/bigmart_volume/")


# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/Volumes/workspace/default/bigmart_volume/BigMart Sales.csv")


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL Schema
# MAGIC

# COMMAND ----------

my_ddl_schema = '''
                 Item_Identifier string,
                 Item_Weight string,
                 Item_Fat_Content string,
                 Item_Visibility double,
                 Item_Type string,
                 Item_MRP double,
                 Outlet_Identifier string,
                 Outlet_Establishment_Year integer,
                 Outlet_Size string,
                 Outlet_Location_Type string,
                 Outlet_Type string,
                 Item_Outlet_Sales double
                '''
              

# COMMAND ----------

df = spark.read.format('csv')\
           .schema(my_ddl_schema)\
           .option('header', 'true')\
           .load("dbfs:/Volumes/workspace/default/bigmart_volume/BigMart Sales.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema = StructType([
        StructField('Item_Identifier', StringType(), True),
        StructField('Item_Weight', StringType(), True),
        StructField('Item_Fat_Content', StringType(), True),
        StructField('Item_Visibility', StringType(), True),
        StructField('Item_Type', StringType(), True),
        StructField('Item_MRP', StringType(), True),
        StructField('Outlet_Identifier', StringType(), True),
        StructField('Outlet_Establishment_Year', StringType(), True),
        StructField('Outlet_Size', StringType(), True),
        StructField('Outlet_Location_Type', StringType(), True),
        StructField('Outlet_Type', StringType(), True),
        StructField('Item_Outlet_Sales', StringType(), True)
])

# COMMAND ----------

df = spark.read.format('csv')\
    .schema(my_struct_schema)\
    .option('header', 'true')\
    .load("dbfs:/Volumes/workspace/default/bigmart_volume/BigMart Sales.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Transformation

# COMMAND ----------

df.display()

# COMMAND ----------

df.select('Item_Identifier', 'Item_Weight', 'Item_Fat_Content').display()

# COMMAND ----------

df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alias

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter/Where transformation

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario-1

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scenario-2
# MAGIC

# COMMAND ----------

df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & col('Outlet_Location_Type').isin('Tier 1', 'Tier 2')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed Transformation

# COMMAND ----------

df.withColumnRenamed('Item_Weight', 'Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###   withColumn Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC # Scenario-1

# COMMAND ----------

df = df.withColumn('flag',lit('New')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 2

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('Multiply', col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 3

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df.withColumn('Item_fat_content', regexp_replace(col('Item_Fat_Content'), 'Regular', 'Reg'))\
    .withColumn('Item_fat_content', regexp_replace(col('Item_Fat_Content'), 'Low Fat', 'LF')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Type Casting

# COMMAND ----------

from pyspark.sql.types import StringType; 

df = df.withColumn('Item_weight',col('Item_weight').cast(StringType()))
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sort/OrderBy Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1

# COMMAND ----------

df.sort(col('Item_weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-2

# COMMAND ----------

df.sort(col('Item_visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-3

# COMMAND ----------

df.display()

# COMMAND ----------

df.sort(['Item_weight', 'Item_visibility'], ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit Transformation

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop function

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### scenario-2
# MAGIC

# COMMAND ----------

df.drop('Item_Visibility', 'Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Duplicates function

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.dropDuplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION and UNION ByName

# COMMAND ----------

# MAGIC %md
# MAGIC Preparing Dataframes

# COMMAND ----------

data1 = [('1','Roshni'),
         ('2','Parashar')]
schema1 = 'id STRING, name STRING'
df1 = spark.createDataFrame(data1, schema1)

data2 = [('3','Gulshan'),
         ('4','Sharma')]
schema2 = 'id STRING, name STRING'
df2 = spark.createDataFrame(data2, schema2)

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union By Name

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### String Functions

# COMMAND ----------

# MAGIC %md
# MAGIC Initcap

# COMMAND ----------

df.select(initcap('Item_Type')).display()

# COMMAND ----------

df.select(lower('Item_Type')).display()

# COMMAND ----------

df.select(upper('Item_Type').alias('Upper_Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Functions

# COMMAND ----------

df = df.withColumn('Current_Date', current_date())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC date Add function

# COMMAND ----------

df = df.withColumn('week_after',date_add('Current_Date', 7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC date Sub Function

# COMMAND ----------

df = df.withColumn('week_before',date_sub('Current_Date', 7))
df.display()

# COMMAND ----------

df = df.withColumn('week_before',date_add('Current_Date', -7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC DateDiff()

# COMMAND ----------

df = df.withColumn('Date_Difference',date_diff('Current_Date', 'week_before'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Date_Format

# COMMAND ----------

df = df.withColumn('week_before',date_format('week_before','dd-MM-yyyy'))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Nulls

# COMMAND ----------

# MAGIC %md
# MAGIC dropping nulls

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.dropna(subset = ['Outlet_Size']).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filling Nulls

# COMMAND ----------

df.fillna('N/A').display()

# COMMAND ----------

df.fillna('N/A',subset = ['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split and Indexing Function

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Indexing

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLODE Function

# COMMAND ----------

df_explode = df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df_explode.display()

# COMMAND ----------

df_explode.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ARRAY_CONTAINS

# COMMAND ----------

df_explode.display()

# COMMAND ----------

df_explode.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group_By

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-1

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP').alias('sum')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-2 

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP').alias('avg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-3

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Sum')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-4

# COMMAND ----------

df.groupBy("Item_Type","Outlet_Size").agg(sum('Item_MRP').alias('Sum'),avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect_List

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book1'),
        ('user2','book3'),
        ('user3','book1')]
schema = 'user string, book string'
df_book = spark.createDataFrame(data, schema)
df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivot

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(sum('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### When-Otherwise

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-1

# COMMAND ----------

df = df.withColumn('Veg_Flag',when(col('Item_Type')=="Meat","Non-Veg").otherwise("Veg")).display()

# COMMAND ----------


dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### left Join

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right join

# COMMAND ----------

df1.join(df2, df1[('dept_id')] == df2[('dept_id')],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti join

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Row Number()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------


window_spec = Window.orderBy(col('Item_Identifier').desc())

df = df.withColumn('rank', rank().over(window_spec)) \
       .withColumn('denseRank', dense_rank().over(window_spec))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cumulative Sum

# COMMAND ----------

df.withColumn('Cumulative_sum', sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df.withColumn('Cumulative_sum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.currentRow))).display()

df.withColumn('TotalSum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### User Defined Functions (UDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Step-1
# MAGIC

# COMMAND ----------

def my_func(x):
    return x + 1

# COMMAND ----------

# MAGIC %md
# MAGIC step-2
# MAGIC

# COMMAND ----------

my_udf = udf(my_func)

# COMMAND ----------

df.withColumn('new_col', my_udf(col('Item_MRP'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

# MAGIC %md
# MAGIC #### csv format

# COMMAND ----------

df.write.format("csv").mode("overwrite").save("dbfs:/Volumes/workspace/default/bigmart_volume/my_csv_file")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing modes

# COMMAND ----------

# MAGIC %md
# MAGIC #### Append

# COMMAND ----------

df.write.format("csv")\
    .mode("append")\
    .save("dbfs:/Volumes/workspace/default/bigmart_volume/my_csv_file")

# COMMAND ----------

df.write.format("csv")\
    .mode("ignore")\
    .save("dbfs:/Volumes/workspace/default/bigmart_volume/my_csv_file")

# COMMAND ----------

df.write.format("csv")\
    .mode("overwrite")\
    .save("dbfs:/Volumes/workspace/default/bigmart_volume/my_csv_file")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet File Format

# COMMAND ----------

df.write.format("parquet").mode("overwrite").save("dbfs:/Volumes/workspace/default/bigmart_volume/my_parquet_file")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create table

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("my_table_file")


# COMMAND ----------

# MAGIC %md
# MAGIC ### MANAGED vs EXTERNAL TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark SQL

# COMMAND ----------

df.createTempView('My_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from My_view where Item_Fat_Content = 'LF'

# COMMAND ----------

df_sql = spark.sql("select * from My_view where Item_Fat_Content = 'LF'")

# COMMAND ----------

df_sql.display()

# COMMAND ----------

