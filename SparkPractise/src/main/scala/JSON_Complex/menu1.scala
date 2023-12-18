package JSON_Complex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._


object menu1 {

  def main(args:Array[String]):Unit={
    
    println("======= Started =======")
    
    val conf = new SparkConf().setAppName("master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    println("===== Reading JSON file =======")
    
    val data = spark.read.format("json")
               .option("multiline", true)
               .load("file:///c:/data/CmJSON/menu1.json")
               
    data.show(truncate=false)
    data.printSchema()
    
    println("======= Flattening the data ======")
    
    val flatdata = data.withColumn("Id", col("menu.id"))
                       .withColumn("POPUP", col("menu.popup.menuitem"))
                       .withColumn("Value", col("menu.value"))
                       .drop("menu")
                       
    flatdata.show(truncate=false)
    flatdata.printSchema()
                       
    val flatdata1 = flatdata.withColumn("MENUITEM", explode(col("POPUP")))
                            .drop("POPUP")
    
    flatdata1.show(truncate=false)
    flatdata1.printSchema()
    
    val flatdata2 = flatdata1.withColumn("Onclick", col("MENUITEM.onclick"))
                       .withColumn("Value1", col("MENUITEM.value"))
                       .drop("MENUITEM")
                       
    flatdata2.show(truncate=false)
    flatdata2.printSchema()
    
    
  }
  
}