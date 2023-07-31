package com.TCS_Assessment.spark

import org.apache.log4j._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable

object TCS_Assessment {

 /** Our main function where the action happens */
  def main(args: Array[String]) {

 // Set the log level to only print errors
 Logger.getLogger("org").setLevel(Level.ERROR)

  // Use new SparkSession interface in Spark 2.0
  val spark = SparkSession.builder().appName("MatchingEngine").getOrCreate()

  // Read the CSV file into a DataFrame
  val input_file = "data/fakefriends.csv"
  val orders_df = spark.read.option("header", "false").option("inferSchema", "true").csv(input_file)
  val orders_df_renamed = orders_df.withColumnRenamed("_c0", "OrderID")
    .withColumnRenamed("_c1", "UserName")
    .withColumnRenamed("_c2", "OrderTime")
    .withColumnRenamed("_c3", "OrderType")
    .withColumnRenamed("_c4", "Quantity")
    .withColumnRenamed("_c5", "Price")

  // Separate DataFrames for BUY and SELL orders
  val buy_orders_df = orders_df_renamed.filter(col("OrderType") === "BUY")
    .withColumnRenamed("Quantity", "BuyQuantity")
    .withColumnRenamed("Price", "BuyPrice")

  val sell_orders_df = orders_df_renamed.filter(col("OrderType") === "SELL")
    .withColumnRenamed("Quantity", "SellQuantity")
    .withColumnRenamed("Price", "SellPrice")

  // Join BUY and SELL orders to find matches based on quantity and best price
  val join_condition = (buy_orders_df("BuyQuantity") === sell_orders_df("SellQuantity")) &&
    (buy_orders_df("BuyPrice") >= sell_orders_df("SellPrice"))

  val matches_df = buy_orders_df.alias("buy").join(sell_orders_df.alias("sell"), join_condition, "inner")
    .select(col("buy.OrderID").alias("BuyOrderID"),
      col("sell.OrderID").alias("SellOrderID"),
      col("buy.OrderTime").alias("MatchTime"),
      col("buy.BuyQuantity").alias("Quantity"),
      col("buy.BuyPrice").alias("Price"))

  // Show the matched data
  matches_df.show()

  // Unmatched data
  buy_orders_df.createOrReplaceTempView("buy_table")
  sell_orders_df.createOrReplaceTempView("sell_table")

  val order_book = spark.sql("""
  SELECT buy_table.OrderID, sell_table.OrderID, buy_table.UserName, buy_table.OrderTime, buy_table.BuyQuantity, buy_table.BuyPrice
  FROM buy_table FULL OUTER JOIN sell_table ON buy_table.BuyQuantity = sell_table.SellQuantity
  WHERE buy_table.UserName IS NULL OR sell_table.UserName IS NULL
""")

  order_book.show()

  // Stop the SparkSession
  spark.stop()

  }
}