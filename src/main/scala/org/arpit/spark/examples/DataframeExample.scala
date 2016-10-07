package org.arpit.spark.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object DataframeExample {

  def main(args: Array[String]): Unit = {
    val filePath = "/home/arathore/scala/workspace/spark-examples/src/main/resources/employee.csv"

    val conf = new SparkConf().setMaster("local[*]").setAppName("DF Example")
    val sqlContext = SQLContext.getOrCreate(new SparkContext(conf))

    Utility.setupLogging()

    val reader = sqlContext.read.format("com.databricks.spark.csv")
    reader.option("path", filePath)
    reader.option("header", "true")

    val df = reader.load
    df.show()
    
    // df.registerTempTable("employee")

    //    val nameCity = sqlContext.sql("select name, city from employee where city='Pune'")
    //    //nameCity.show()
    //
    //    val groupBy = sqlContext.sql("select city, avg(salary) as avg_salary from employee group by city")
    //    groupBy.show()
    //    
    //    val dfw = groupBy.coalesce(1).write.mode("append").format("json")
    //    dfw.save("/home/arathore/Desktop/emp")
  }
}