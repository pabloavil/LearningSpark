package capitulo2
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.Time

object MM {

case class RegistroMM(State:String,Color:String,Count:BigInt){

}


def main(args: Array[String]) {
	Logger.getLogger("org").setLevel(Level.ERROR)
	// Create a SparkContext using every core of the local machine


	val spark = SparkSession.builder().getOrCreate()
	import spark.implicits._


	val mM = spark.read.format("csv")
	.option("header",true)
	.option("inferSchema", "true")
	.load("mnm_dataset.csv")

	val mMDS =mM.as[RegistroMM]

	mM.createOrReplaceTempView("MM")             

	//ver tiempos
	mM.show()
	spark.sql("Select * From MM").show()

	val countMM =mM.groupBy("State", "Color").agg(count("count").alias("Count")).orderBy("Color")
	countMM.show()

	val countMMDS = mMDS.groupBy("State", "Color").agg(count("count").alias("Count")).orderBy("Color").as[RegistroMM]
			countMMDS.show()

	spark.sql("Select State, Color , count(count) as Total from MM Group BY State ,Color Order By Color ").show()


	val caTxCountMnNDF = mM.select("*") //Si seleccionas todo creo que no es necesaria la clausula
	.where(col("State") === "CA" ||col("State") === "TX" ) //Y asi todos los estrados que quieras
	.groupBy("State", "Color")
	.agg(count("Count")
			.alias("Total"))
	.orderBy(desc("Total"))

	caTxCountMnNDF.show(10)	

	spark.sql("Select State, Color , count(count) as Total From MM Where State IN ('TX','CA','CO') Group BY State ,Color Order By Color ").show()
	//		
	//Vamos a calcular  Min, Max, Avg y Count en una sola sentencia

	mM.groupBy("State", "Color").agg(min($"Count").alias("Min"), max("count").alias("Max"),round(avg("count"),2).alias("Media"),count("count").alias("Contador")).show()
	mMDS.groupBy("State", "Color").agg(min($"Count").alias("Min"), max("count").alias("Max"),round(avg("count"),2).alias("Media"),count("count").alias("Contador")).show()


}

}