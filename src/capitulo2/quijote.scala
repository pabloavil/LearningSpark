package capitulo2

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession



object quijote {
	def main(args: Array[String]) {
		Logger.getLogger("org").setLevel(Level.ERROR)

		// Create a SparkContext using every core of the local machine

		val spark = SparkSession.builder().appName("quijote").getOrCreate()



		val quijote = spark.read.text("el_quijote.txt")


		quijote.show(false)       //muestra 20 filas sin truncar 
		quijote.show(true)        //20 filas true trunca a 20 caracteres y alinea a derecha (por defecto)
		quijote.show(10)          //num filas
		quijote.show(10,true)     //num filas ,true trunca a 20 caracteres y alinea a derecha
		quijote.show(10,10)       //numero filas, truncate strings (0 no truca)

		quijote.show(10,10,true)  //numero filas, truncate strings (0 no truca) muestra cada atributo debajo del anterior

		println( quijote.count())

		println( quijote.head()) //Coje las primeras x filas , por defecto 1 return Row
		quijote.head(10).foreach(println)  //Return Array[Row]
		quijote.first() //Es un alias de .head() pero no admite parametros
		quijote.take(12) //Manda al drivcer las x primeras filas CUIDADOOOO Puede dar error de memoria

	}
}