package csv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

case class tschema(n1:String,n2:String,n3:String)

object list_to_DF {
  
  
  def main(args:Array[String]):Unit={
    
    println("===== Started ======")
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("Error")
			
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
    
    val liststr = List("a,b,c","c,d,e")
    liststr.foreach(println)
    
    val sdata = liststr.map(x => x.split(","))
    sdata.foreach(println)
    val df1 = sdata.toDF().show()
    
    val schemadata = sdata.map(x => tschema(x(0),x(1),x(2)))
    schemadata.foreach(println)
    
   val df = schemadata.toDF()
   df.show()
    
  }
}