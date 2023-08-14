from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
import datetime
import logging
import sys
from pyspark.sql.functions import year, month, dayofmonth
from create_spark import create_spark_object
from validate import get_current_date
from ingest import load_files, display_df, df_count

logging.config.fileConfig('properties/configuration/logging.config')


def main():
    try:
        logging.info('I am the main method')
        logging.info('calling spark object')
        spark = create_spark_object()

        #logging.info('object create...', str(spark))
        logging.info('validating spark object')
        get_current_date(spark)

        logging.info('reading file....')
        df_tsl = load_files(spark, file_dir='/home/utkarsh/Downloads/archive/TSLA.csv')
        df_google = load_files(spark, file_dir='/home/utkarsh/Downloads/archive/GOOG.csv')
        df_amzn = load_files(spark, file_dir='/home/utkarsh/Downloads/archive/AMZN.csv')
        logging.info('validating the dataframes... ')
        df_count(df_tsl,'df_tsl')
        df_count(df_google, 'df_google')
        df_count(df_amzn, 'df_amzn')

        # Average closing price per year for AMZN
        df_tsl.select(year("Date").alias("year"), "AdjClose") \
            .groupby("year") \
            .avg("AdjClose") \
            .alias("avg") \
            .sort("year") \
            .show()
        # average closing price per month for apc

        df_amzn.select(year("Date").alias("year"),
                       month("Date").alias("month"),
                       "AdjClose").groupby("year", "month") \
            .avg("AdjClose").sort("year", "month").show()

        df_amzn.createOrReplaceTempView("amazon_stocks")
        df_google.createOrReplaceTempView("google_stocks")
        df_tsl.createOrReplaceTempView("tesla_stocks")

        # average closing price per month for XOM ordered by year,month

        spark.sql("""SELECT year(amazon_stocks.Date) as yr, month(amazon_stocks.Date) as mo, avg(amazon_stocks.AdjClose) 
                        from amazon_stocks group By year(amazon_stocks.Date), month(amazon_stocks.Date)""").show()

        # closing price for SPY go up or down by more than 2 dollars

        spark.sql("SELECT google_stocks.Date,"
                  " google_stocks.Open,"
                  " google_stocks.Close,"
                  " abs(google_stocks.Close - google_stocks.Open) as spydif "
                  "FROM google_stocks"
                  " WHERE abs(google_stocks.Close - google_stocks.Open) > 4 ").show()

        # max, min closing price for SPY and XOM by Yea

        spark.sql("SELECT year(tesla_stocks.Date) as yr,"
                  " max(tesla_stocks.AdjClose),"
                  " min(tesla_stocks.AdjClose)"
                  " FROM tesla_stocks group By year(tesla_stocks.Date)").show()

        # Join all stock closing prices in order to compare

        joinclose = spark.sql("SELECT tesla_stocks.Date,"
                              " tesla_stocks.AdjClose as teslaclose,"
                              " amazon_stocks.AdjClose as amazonclose,"
                              " google_stocks.AdjClose as googleclose"
                              " from tesla_stocks join google_stocks"
                              " on tesla_stocks.Date = google_stocks.Date"
                              " join amazon_stocks on tesla_stocks.Date = amazon_stocks.Date").cache()

        joinclose.show()

        joinclose.createOrReplaceTempView("joinclose")

        spark.sql("SELECT year(joinclose.Date) as yr,"
                  " avg(joinclose.teslaclose) as teslaclose,"
                  " avg(joinclose.amazonclose) as amazonclose,"
                  " avg(joinclose.googleclose) as googleclose"
                  " from joinclose group By year(joinclose.Date)"
                  " order by year(joinclose.Date)").show()

        joinclose.write.format("parquet").save("joinstocks.parquet")
        final_df = spark.read.parquet("joinstocks.parquet")
        final_df.show()

    except Exception as e:
        logging.error('An error occured ===', str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
    logging.info('Application done')
