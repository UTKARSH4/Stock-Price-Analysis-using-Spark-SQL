from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
import datetime
from pyspark.sql.functions import year, month, dayofmonth
spark = SparkSession \
        .builder \
        .appName("stock_Price") \
        .master("local[*]") \
        .getOrCreate()

df_tsl = spark.read.format("csv")\
        .option("header", True)\
        .option("inferSchema", True).load("C:/Users/utkarsh.verma/Downloads/TSLA.csv")

df_google = spark.read.format("csv").\
        option("header", True).\
        option("inferSchema", True).load("C:/Users/utkarsh.verma/Downloads/GOOG.csv")

df_amzn = spark.read.format("csv").\
        option("header", True).\
        option("inferSchema", True).load("C:/Users/utkarsh.verma/Downloads/AMZN.csv")

# schema = StructType([
#    StructField("date", StringType(), True),
#    StructField("openprice", IntegerType(), True),
#    StructField("highprice", IntegerType(), True),
#    StructField("lowprice", IntegerType(), True),
#    StructField("closeprice", IntegerType(), True),
#    StructField("volume", IntegerType(), True),
#    StructField("adjcloseprice", IntegerType(), True)])
df_amzn.printSchema()

# Average closing price per year for AMZN
df_tsl.select(year("Date").alias("year"), "AdjClose")\
    .groupby("year")\
    .avg("AdjClose")\
    .alias("avg")\
    .sort("year")\
    .show()
# average closing price per month for apc

df_amzn.select(year("Date").alias("year"),
                month("Date").alias("month"),
                "AdjClose").groupby("year", "month")\
                .avg("AdjClose").sort("year", "month").show()

df_amzn.createOrReplaceTempView("amazon_stocks")
df_google.createOrReplaceTempView("google_stocks")
df_tsl.createOrReplaceTempView("tesla_stocks")

# average closing price per month for XOM ordered by year,month

spark.sql("""SELECT year(amazon_stocks.Date) as yr, month(amazon_stocks.Date) as mo, avg(amazon_stocks.AdjClose) 
                from amazon_stocks group By year(amazon_stocks.Date), month(amazon_stocks.Date)""").show()

#closing price for SPY go up or down by more than 2 dollars

spark.sql("SELECT google_stocks.Date,"
               " google_stocks.Open,"
               " google_stocks.Close,"
               " abs(google_stocks.Close - google_stocks.Open) as spydif "
               "FROM google_stocks"
               " WHERE abs(google_stocks.Close - google_stocks.Open) > 4 ").show()

#max, min closing price for SPY and XOM by Yea

spark.sql("SELECT year(tesla_stocks.Date) as yr,"
               " max(tesla_stocks.AdjClose),"
               " min(tesla_stocks.AdjClose)"
               " FROM tesla_stocks group By year(tesla_stocks.Date)").show()

# Join all stock closing prices in order to compare

joinclose=spark.sql("SELECT tesla_stocks.Date,"
                         " tesla_stocks.AdjClose as teslaclose,"
                         " amazon_stocks.AdjClose as amazonclose,"
                         " google_stocks.AdjClose as googleclose"
                         " from tesla_stocks join google_stocks"
                         " on tesla_stocks.Date = google_stocks.Date"
                         " join amazon_stocks on tesla_stocks.Date = amazon_stocks.Date").cache()

joinclose.show()

joinclose.registerTempTable("joinclose")

spark.sql("SELECT year(joinclose.Date) as yr,"
               " avg(joinclose.teslaclose) as teslaclose,"
               " avg(joinclose.amazonclose) as amazonclose,"
               " avg(joinclose.googleclose) as googleclose"
               " from joinclose group By year(joinclose.Date)"
               " order by year(joinclose.Date)").show()

joinclose.write.format("parquet").save("joinstocks.parquet")
final_df = spark.read.parquet("joinstocks.parquet")
final_df.show()
