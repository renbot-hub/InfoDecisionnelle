import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util.Properties
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

// Mapping des types
class OracleDialect extends JdbcDialect {
    override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
      case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.INTEGER))
      case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.INTEGER))
      case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.BIGINT))
      case FloatType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.FLOAT))
      case DoubleType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.DOUBLE))
      case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
      case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
      case StringType => Some(JdbcType("VARCHAR2(4000)", java.sql.Types.VARCHAR))
      case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))
      //Ligne ajoutée pour timestamp
      case TimestampType => Some(JdbcType("TIMESTAMP",java.sql.Types.TIMESTAMP))
      case _ => None
    }
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle")
}

object SimpleApp {
	def main(args: Array[String]) {
		var debug_limit = 10
		var granularite_review_count = 10

		/**********************************************
		*           FONCTIONS UTILITAIRES             *
		***********************************************/

		// --- find_review_count_category : Associe un review_count à une catégorie de popularité définie
		// Dictionnaire des catégories de popularité
		var map_review_count = Map("impopulaire" -> 0,	
		"peu populaire" -> 10,
		"populaire" -> 100,
		"très populaire" -> 500,
		"extrêmement populaire" -> 1000)

		// Fonction mappant un review_count à une catégorie du dictionnaire
		def find_review_count_category(review_count: Int): String = {
			var category = "impopulaire"
			for ((k,v) <- map_review_count) {
				if (review_count >= v) {
					category = k
				}
			}
			return category
		}
		val findReviewCountCategoryUDF = udf(find_review_count_category _)

		// ----- SMART KEYS : fonctions pour générer des clés à partir de dates et heures

		// --- smart_key_date : fonction prenant en entrée une date au format yyyy-mm-dd
		// et renvoie une smart key yyyymmdd pour la date
		// si le format en entrée n'est pas correct, renvoie null
		def smart_key_date(date: String): String = {
			var smart_key = ""
			if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
				smart_key = date.replaceAll("-", "")
			}
			smart_key
		}
		val smartKeyDateUDF = udf(smart_key_date _)


		// --- smart_key_time : fonction prenant en entrée une heure au format hh:mm
		// et renvoie une smart key hhmm pour l'heure
		// si le format en entrée n'est pas correct, renvoie null
		def smart_key_time(time: String): String = {	
			var smart_key = "" 
			if (time.matches("\\d{2}:\\d{2}:\\d{2}")) { 
				val Array(hh, mm, _) = time.split(":") 
				smart_key = hh + mm 
			}
			smart_key
		}
		val smartKeyTimeUDF = udf(smart_key_time _)




		/**********************************************
		*              INITIALISATION                 *
		***********************************************/


		// --- Initialisation de Spark
		val spark = SparkSession.builder
			.appName("ETL")
			.master("local[4]")
			.config("spark.executor.memory", "32g") 
			.config("spark.driver.memory", "10g")   
			.getOrCreate()

		// --- Chemin des fichiers JSON : 	chargement des tables business et checkin
		val checkinFile 	= 	"/travail/ep298479/dataset/yelp_academic_dataset_checkin.json"
		val businessFile 	= 	"/travail/ep298479/dataset/yelp_academic_dataset_business.json"

		// --- Connexion DB PostgreSQL 	: 	chargement des tables user, review, elite
		val urlPostgreSQL = "jdbc:postgresql://stendhal:5432/tpid2020"
		val connectionProporetiesPostgreSQL = new Properties()
		connectionProporetiesPostgreSQL.setProperty("driver", "org.postgresql.Driver")
		connectionProporetiesPostgreSQL.put("user", "tpid")
		connectionProporetiesPostgreSQL.put("password", "tpid")

		// --- Connexion DB Oracle 		: 	écriture des tables de dimension et de fait
		Class.forName("oracle.jdbc.driver.OracleDriver")
		val url = "jdbc:oracle:thin:@stendhal:1521:enss2023"
		import java.util.Properties
		val connectionProperties = new Properties()
		connectionProperties.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
		connectionProperties.setProperty("user", "ep298479")
		connectionProperties.setProperty("password","ep298479")



		/**********************************************
		*          EXTRACTION DES DONNEES             *
		***********************************************/


		// --- Extraction depuis la base de données PostgreSQL
		// Partitions sur yelping_since pour user, et spark_partition pour review
		// a des fins de performance
		var user = spark.read
			.option("numPartitions", 10)
			.option("partitionColumn", "yelping_since")
			.option("lowerBound", "2010-01-01")
			.option("upperBound", "2019-12-31")
			.jdbc(urlPostgreSQL, "yelp.user", connectionProporetiesPostgreSQL)

		var review = spark.read
			.option("numPartitions", 100)
			.option("partitionColumn", "spark_partition")
			.option("lowerBound", "0")
     		.option("upperBound", "99")
			.jdbc(urlPostgreSQL, "yelp.review", connectionProporetiesPostgreSQL)

		var elite = spark.read
			.option("user", "tpid")
			.option("password", "tpid")
			.jdbc(urlPostgreSQL, "yelp.elite", connectionProporetiesPostgreSQL)


		// --- Extraction depuis les fichiers JSON
		var business = spark
			.read
			.json(businessFile)

		var checkin = spark
			.read
	  		.option("header",true)
			.json(checkinFile)

		/**********************************************
		*       PREPARATION DES TABLES DE FAIT        *
		***********************************************/


		// --- FACT TABLE 1 : fact_review
		// Jointure avec les dimensions dim_user, dim_business, dim_date
		// - Clés étrangères : user_id, business_id, date_id
		// - Mesures : stars, useful, funny, cool, word_count

		var fact_review = review
			.select("review_id", "user_id", "business_id", "date", "stars", "useful", "funny", "cool","text")

			// Ajout d'une mesure : word_count (nombre de mots dans le texte)
			.withColumn("word_count", size(split(col("text"), " ")))
			.drop("text")

			// Renommage des mesures avec le suffixe "_measure"
			.withColumnRenamed("stars", "stars_measure")
			.withColumnRenamed("useful", "useful_measure")
			.withColumnRenamed("funny", "funny_measure")
			.withColumnRenamed("cool", "cool_measure")
			.withColumnRenamed("word_count", "word_count_measure")

			// Conversion de la date (yyyy-mm-dd) en smart key (yyyymmdd)
			.withColumn("date_id", smartKeyDateUDF(col("date")))		
			.drop("date")												
			.na.drop("any", Seq("date_id"))
			.cache()


		// --- FACT TABLE 2 : fact_checkin
		// Jointure avec les dimensions dim_business, dim_date, dim_time
		// - Clés étrangères : business_id, date_id, time_id
		// - Mesures : aucune

		var fact_checkin = checkin

			// Explosion de la colonne date en deux colonnes date_id et time_id
			.withColumn("date", split(col("date"), ","))
			.withColumn("date", explode(col("date")))
			.withColumn("date", trim(col("date")))
			.withColumn("date_id", split(col("date"), " ").getItem(0))
			.withColumn("time_id", split(col("date"), " ").getItem(1))
			.drop("date")

			// Conversion de la date (yyyy-mm-dd) en smart key (yyyymmdd)
			// Conversion de l'heure (hh:mm:ss) en smart key (hhmm)
			.withColumn("date_id", smartKeyDateUDF(col("date_id")))
			.withColumn("time_id", smartKeyTimeUDF(col("time_id")))
			.na.drop("any", Seq("date_id", "time_id"))

			// Clé primaire auto-incrémentée
			.withColumn("checkin_id", monotonically_increasing_id())
			.cache()
			


		/**********************************************
		*     PREPARATION DES TABLES DE DIMENSION     *
		***********************************************/


		// --- DIM TABLE 1 : dim_user
		// 3 colonnes originales : user_id, yelping_since, review_count
		// 1 colonne ajoutée : elite (1 si l'utilisateur a déjà été élite, 0 sinon)

		// Table contenant les user_id des utilisateurs ayant déjà été élite
		var elite_user = elite
			.select("user_id")
			.withColumnRenamed("user_id", "fk_user_id")
			.distinct()

		// Création de la dimension
		var dim_user = user
			.select("user_id", "yelping_since", "review_count", "average_stars")
			.join(
				elite_user,
				user("user_id") === elite_user("fk_user_id"),
				"left"
			)
			.withColumn("elite", when(col("fk_user_id").isNotNull, 1).otherwise(0))
			.drop("fk_user_id")	

		
		// --- DIM TABLE 2 : dim_business
		// 7 colonnes originales : business_id, name, address, city, state, stars, review_count, latitude, longitude
		// 4 colonnes ajoutées : restaurant (1 si le business est un restaurant, 0 sinon), shopping (1 si le business est un magasin, 0 sinon), popularity (catégorie de popularité), checkin_count (nombre total de checkins)
 		var dim_business = 
			business
			.select("business_id", "name", "address", "city", "state", "stars", "review_count", "categories", "latitude", "longitude")

			// Ajout de colonnes restaurant et shopping correspondant à des booléens
			.withColumn("restaurant", when(col("categories").contains("Restaurants"), 1).otherwise(0))
			.withColumn("shopping", when(col("categories").contains("Shopping"), 1).otherwise(0))

			// Conversion de review_count en catégorie de popularité
			.withColumn(
				"popularity",
				when(col("review_count") < 10, "impopulaire")
				.when(col("review_count") < 100, "peu populaire")
				.when(col("review_count") < 500, "populaire")
				.when(col("review_count") < 1000, "tres populaire")
				.otherwise("extremement populaire"))
			.drop("categories")

			// Ajout de la mesure checkin_count
			.withColumnRenamed("business_id", "fact_business_id")
			.join(
				fact_checkin.groupBy("business_id").count(),
				col("business_id") === col("fact_business_id"),
				"left"
			)
			.withColumnRenamed("count", "checkin_count")
			.drop("fact_business_id")

		// --- DIM TABLE 3 : dim_date
		// 1 colonne originale : date_id
		// 4 colonnes ajoutées : year, month, day, day_of_week

		// Récupérer toutes les dates uniques des fact tables
		val dates_review = fact_review.select("date_id").distinct()
		val dates_checkin = fact_checkin.select("date_id").distinct()
		val all_dates = dates_review.union(dates_checkin).distinct()

		// Définir un schéma pour dim_date sinon il y a des problèmes
		val schema = StructType(
			Array(
				StructField("date_id", StringType, nullable = false),
				StructField("year", IntegerType, nullable = false),
				StructField("month", IntegerType, nullable = false),
				StructField("day", IntegerType, nullable = false),
				StructField("day_of_week", StringType, nullable = false)
			)
		)

		// Création de la dimension
		val dim_date = all_dates.withColumn("year", substring(col("date_id"), 1, 4).cast(IntegerType))
		.withColumn("month", substring(col("date_id"), 5, 2).cast(IntegerType))
		.withColumn("day", substring(col("date_id"), 7, 2).cast(IntegerType))
		.withColumn("date", to_date(col("date_id"), "yyyyMMdd"))
		.withColumn("day_of_week", date_format(col("date"), "E"))
		.select("date_id", "year", "month", "day", "day_of_week")

		// --- DIM TABLE 4 : dim_time
		// 1 colonne originale : time_id
		// 2 collones ajoutées : hour, minute

		// Pas besoin de jointure pour dim_time
		// Seulement 1440 lignes, on peut les générer directement
		var dim_time = spark.range(0, 24*60, 1)
			.withColumn("time_id", lpad(col("id"), 4, "0"))
			.drop("id")
			.withColumn("hour", substring(col("time_id"), 1, 2).cast(IntegerType))
			.withColumn("minute", substring(col("time_id"), 3, 2).cast(IntegerType))

		/**********************************************
		*       ECRITURE DANS LA BASE DE DONNEES      *
		***********************************************/


		// Mapping des types

		val dialect = new OracleDialect
    	JdbcDialects.registerDialect(dialect)


		// Ecriture des tables de dimension
		
		dim_business.write
			.mode(SaveMode.Overwrite)
			.jdbc(url, "dim_business_raw", connectionProperties)

		dim_user.write
			.mode(SaveMode.Overwrite)
			.jdbc(url, "dim_user_raw", connectionProperties)

		dim_date.write
			.mode(SaveMode.Overwrite)
			.jdbc(url, "dim_date_raw", connectionProperties)

		dim_time.write
			.mode(SaveMode.Overwrite)
			.jdbc(url, "dim_time_raw", connectionProperties)

		
		// Ecriture des tables de fait

		fact_review.write
			.mode(SaveMode.Overwrite)
			.jdbc(url, "fact_review_raw", connectionProperties)

		fact_checkin.write
			.mode(SaveMode.Overwrite)
			.jdbc(url, "fact_checkin_raw", connectionProperties)

    spark.stop()
	}
}


