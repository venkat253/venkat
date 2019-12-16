package org.inceptez.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql._;
case class customercaseclass(custid:Int,custfname:String,custlname:String,custage:Int,custprofession:String)
object sql1 {
 //https://spark.apache.org/docs/latest/api/sql/index.html
  //https://spark.apache.org/docs/2.0.1/api/java/org/apache/spark/sql/DataFrameReader.html
  def main(args:Array[String])
  {
    
/*
SQLContext is a class and is used for initializing the functionalities of Spark SQL. 
SparkContext class object (sc) is required for initializing SQLContext class object.
 */
    val conf = new SparkConf().setAppName("SQL1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    sc.setLogLevel("ERROR");

/*Spark 2.x, we have a new entry point for DataSet and Dataframe API’s called as Spark Session.
SparkSession is essentially combination of SQLContext, HiveContext and future StreamingContext. 
All the API’s available on those contexts are available on spark session also. 
Spark session internally has a spark context for actual computation.*/

    
val spark=SparkSession.builder().appName("Sample sql app").master("local[*]").getOrCreate();

    // Various ways of creating dataframes

// 1.Create dataframe using case class  with toDF and createdataframe functions (Reflection) 
// 2.create dataframe from collections type such as structtype and fields        
// 3.creating dataframe from read method using csv  and other modules like json, orc, parquet etc.
// 4.Create dataframe from hive.
// 5.Create dataframe from DB using jdbc.

    val arr:List[(String,Int)] = List(("Tiger",20),("Lion",10),("Elephant",40),("Leopard",12))
    //"Tiger"|20
    //"Lion"|10
    //"Elephant"|40
    //"Leopard"|12
    // val rdd1= sc.textFile(file).map(x=>x.split("|")).map(x=>(x(1).toInt)).sum -> List(("Tiger",20),("Lion",10),("Elephant",40),"Leopard",12)
    //val df1=spark.read.option("delimiter","|")..option("inferschema","true").csv("location").toDF("name","cnt") --> df
    //df1.agg(sum(col("cnt"))) - medium
    //df1.createOrReplaceTempView("v1") -simple
    //spark.sql("select sum(cnt) from v1").show
    val rdd = sc.parallelize(arr)
    import sqlc.implicits._
    
    val df = arr.toDF("animalname","count")
     
    println("Printing DF1 List to DF Example");
  
    df.select("count","animalname").show(10);
    df.printSchema()
    df.show()
    
    /* Create dataframe using case class  with toDF and createdataframe functions*/
    println("Create dataframe using case class from file")
    
    //step 1: import the implicits
    //import sql.implicits._
    
    //step 2: create rdd
    val filerdd = sc.textFile("file:/home/hduser/hive/data/custs")
    
    //step 3: create case class based on the structure of the data
val rdd1 = filerdd.map(x => x.split(",")).filter(x => x.length == 5)
.map(x => customercaseclass(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    
println("Creating Dataframe and DataSet")

    val filedf = rdd1.toDF()
    val fileds=rdd1.toDS();
    
    //dataframe=dataset[row]
    //dataset=dataset[caseclass]
    
    //implicits is not required for the createDataFrame function.
    val filedf1 = sqlc.createDataFrame(rdd1)
    val fileds1 = sqlc.createDataset(rdd1)
    
    filedf.printSchema()
    filedf.show(10,false)
    
    //creating dataframe from createdataframe
    
    println("creating dataframe using createdataframe applying structure type")
    
    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.types._
    
val rddrow = filerdd.map(x => x.split(",")).filter(x => x.length == 5)
.map(x => Row(x(0).toInt,x(1),x(2),x(3).toInt,x(4)))
    
// use collections, since a schema is built as a StructType(Array[StructField]). 
//So it's basically a choice between tuples and collections. 

    val structypeschema = StructType(List(StructField("custid", IntegerType, true),
        StructField("custfname", StringType, true),
        StructField("custlname", StringType, true),
        StructField("custage", IntegerType, true),
        StructField("custprofession", StringType, true)))
    
   val dfcreatedataframe = sqlc.createDataFrame(rddrow, structypeschema)
   
   println("DSL Queries on DF")
       
   dfcreatedataframe.select("*").filter("custprofession='Pilot' and custage>35").show(10); 
   
    println("DSL Queries on DS")
   fileds.select("*").filter("custprofession='Pilot' and custage>35").show(10);
  
    println("Registering Dataset as a temp view and writing sql queries")
    fileds.createOrReplaceTempView("datasettempview")
    spark.sql("select * from datasettempview where custprofession='Pilot' and custage>35").show(5,false)
    
    //creating dataframe from read method using csv
    println("creating dataframe from read method using csv and renaming column names explicitly")
    
    val dfcsv = sqlc.read.format("csv")
    //.option("header",true)
    .option("delimiter",",")
    .option("inferSchema",true)
    .load("file:/home/hduser/hive/data/custs")

    dfcsv.show(10);
    dfcsv.printSchema();
    
    val onecoldfcsv=dfcsv.withColumnRenamed("_c0", "custid")
    val onecoldfcsv1=dfcsv.withColumnRenamed("_c0", "custid").drop("_c1","_c2")
    
    dfcsv.createOrReplaceTempView("customer")
    
    println("Converting DSL Equivalent SQL queries")
    
    spark.sql(""" select _c0 as custid,_c3,_c4 from customer """).show(5,false)    
    
    import org.apache.spark.sql.functions._
 //https://spark.apache.org/docs/2.3.0/api/sql/
    val withcoldf = dfcsv.withColumn("pilot",$"_c4".contains("Pilot"))
    .withColumn("Dataset",lit("Transactions")).withColumn("fullname",concat(col("_c1"),lit(" "),$"_c2"))
    // consider a column $/'/col()
    println("withcoldf")
    withcoldf.show(5,false)
    //val df$$=sql(select concat(1,"$$",2,"$$",3,"$$",4)).write.csv("")
    //dsrenamed.withColumn("new",col("daystolive")+1).select("new","daystolive").show
    //1,irfan, 2,basith -> {"id":1,"name":"IRFAN"}
    // spark.read.csv() -> df1.withColumnRenamed("_c0","id").wcr(_c1,name).withColumn("uppername",upper(`name))
    //.drop("name").withColumn().write.json
    
    val casttostring=dfcsv.withColumn("longage",$"_c3".cast("String"))
    
    println("casttostring")
    casttostring.show()
    
    val multiplecoldfcsv=dfcsv.select($"_c0".alias("custid"),$"_c1".alias("Custname"))
    
    println("SQL EQUIVALENT to DSL")
    spark.sql(""" select case when _c4 like ('%Pilot%') then true else false end as pilot,
                         'Transactions' as Dataset,concat(_c1,' ',_c2) as fullname,
                         cast(_c3 as string) as longage, _c0 as custid, _c1 as Custname
                          from customer """).show(5,false)
    //val classobj=new org.inceptez.spark.allfunctions
    println("Applying UDF in DSL")
    import org.apache.spark.sql.functions._
    val dsludf = udf(trimupperdsl _)
    dfcsv.withColumn("udfapplied",dsludf('_c1)).show(5,false)
    
    val onecoldfcsvexplain=dfcsv.withColumnRenamed("_c0", "custid").drop("_c1","_c2").explain(true)
    
        
    println("dfcsv")
    dfcsv.printSchema();
    println("onecoldfcsv")
    onecoldfcsv.printSchema();
    println("onecoldfcsv1")
    onecoldfcsv1.printSchema();
    println("multiplecoldfcsv")
    multiplecoldfcsv.printSchema();
    //creating dataframe from read method using csv with columns explicitly defined
    println("creating dataframe from read method using csv with columns explicitly defined")
    
    val dfcsvcolumns = sqlc.read.format("csv")
    //.option("header",true)
    .option("delimiter",",")
    .option("inferSchema",true)
    .load("file:/home/hduser/hive/data/custs")
    .toDF("custid","custfname","custlname","custage","custprofession")
  
    val dfgrouping = dfcsvcolumns.groupBy("custprofession")
    .agg(max("custage").alias("maxage"),min("custage").alias("minage"))
    println("dfgrouping")
    dfgrouping.show(5,false)
    
    dfcsvcolumns.createOrReplaceTempView("customer")

    spark.sql("""select custprofession,max(custage) as maxage,min(custage) as minage 
                  from customer 
                  group by custprofession""").show(5,false)
    
    println("Example for spark sql udf function registration with method overloading concept")
    import org.apache.spark.sql.functions._;
    
    //val dfreadyudf=udf(trimupperdsl _)
    
    spark.udf.register("trimupper",trimupperdsl _)
    
    val udf1=(x:String)=>trimupper(x)
    val udf2=(x:String,y:String)=>trimupper(x,y)
    
    spark.udf.register("udff1",udf1)
    spark.udf.register("udff2",udf2)

    sqlc.sql("""select udff1(custprofession) as custprofupper,udff2(custfname,custprofession) as custnameprofession 
             from customer limit 10""").show(10,false)    
    val df1 = sqlc.sql("""select custprofession, count(custid) from customer 
                        where custage > 50 group by custprofession""")
    
    val df2 = sqlc.sql("""select custid,custfname,custlname,case when (custage < 40) then 'Middle Aged' else 'Old Age' end as agegrp 
                        from customer""")
    
    println("Spark SQL customer info")                        
    df2.show()
    
    
    
    val txns=spark.read.option("inferschema",true)
    .option("header",false).option("delimiter",",")
    .csv("file:///home/hduser/hive/data/txns")
    .toDF("txnid","dt","cid","amt","category","product","city","state","transtype")
    
txns.show(10);    


    
println("Join Scenario in temp views")
txns.createOrReplaceTempView("trans");
    
val dfjoinsql = sqlc.sql("""select a.custid,custprofession,b.amt,b.transtype  
  from customer a inner join trans b
  on a.custid=b.cid""")
  
// AQ (TYPE, OPTIONS, METHOD) -> CURE (SAN,CL,SCR,NOR,CLEAN,PREP,PREPROC) -> LKP, ENRICH, JOIN -> AGG, WRANGLE, MUNGE -> STORE (TYPES, OPTIONS, METHOD)
// DATALAKE -> RAW, INGES,STAGING,  FILTER, CURATION
  dfjoinsql.show(10)

  }
 
  def trimupperdsl(i:String):String=
    {return i.trim().toUpperCase()}
  
  def trimupper(i:String):String=
    {return i.trim().toUpperCase()}
  
  def trimupper(i:String,j:String):String=
    {return i.trim().toUpperCase()+" " +j.trim().toUpperCase()}
}









