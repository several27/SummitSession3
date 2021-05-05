package graph

import org.apache.spark.sql.types._
import io.prophecy.libs._
import io.prophecy.libs.UDFUtils._
import io.prophecy.libs.Component._
import io.prophecy.libs.DataHelpers._
import io.prophecy.libs.SparkFunctions._
import io.prophecy.libs.FixedFileFormatImplicits._
import org.apache.spark.sql.ProphecyDataFrame._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._

@Visual(id = "Report", label = "Report", x = 894, y = 276, phase = 0)
object Report {

  @UsesDataset(id = "1802", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    val fabric = Config.fabricName
    fabric match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id", IntegerType, false),
            StructField("first_name",  StringType,  false),
            StructField("last_name",   StringType,  false),
            StructField("amount",      DoubleType,  false),
            StructField("full_name",   StringType,  false)
          )
        )
        in.write
          .format("parquet")
          .mode("overwrite")
          .save("dbfs:/DatabricksSession/Report.parq")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
