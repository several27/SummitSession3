
package graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.libs.SparkTestingUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.junit.Assert
import org.scalatest.FunSuite
import java.time.LocalDateTime
import org.scalatest.junit.JUnitRunner
import java.sql.{Date, Timestamp}

@RunWith(classOf[JUnitRunner])
class With_FullNameTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Test Order_status for out predicates: order_status") {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    val dfIn = inDf(Seq("customer_id", "first_name", "last_name", "amount", "order_status"), Seq(
      Seq[Any]("26".toInt,"Waite","Petschelt","586.08".toDouble,"Finished"),
      Seq[Any]("26".toInt,"Waite","Petschelt","9.85".toDouble,"Started"),
      Seq[Any]("63".toInt,"Constance","Sleith","287.5".toDouble,"Finished"),
      Seq[Any]("35".toInt,"Viva","Schulke","514.19".toDouble,"Finished"),
      Seq[Any]("31".toInt,"Barrett","Amies","154.41".toDouble,"Finished"),
      Seq[Any]("51".toInt,"Kit","Skamell","924.89".toDouble,"Started"),
      Seq[Any]("35".toInt,"Viva","Schulke","198.05".toDouble,"Started"),
      Seq[Any]("100".toInt,"Gillan","Heritege","121.03".toDouble,"Finished"),
      Seq[Any]("94".toInt,"Ogdan","Bussetti","551.87".toDouble,"Started"),
      Seq[Any]("62".toInt,"Homer","Lindstedt","343.59".toDouble,"Finished"),
      Seq[Any]("3".toInt,"Marjy","Della","60.72".toDouble,"Approved"),
      Seq[Any]("100".toInt,"Gillan","Heritege","449.74".toDouble,"Finished"),
      Seq[Any]("94".toInt,"Ogdan","Bussetti","915.25".toDouble,"Approved"),
      Seq[Any]("84".toInt,"Balduin","Birdsall","726.97".toDouble,"Started"),
      Seq[Any]("31".toInt,"Barrett","Amies","607.46".toDouble,"Approved"),
      Seq[Any]("10".toInt,"Jonis","Bigly","174.29".toDouble,"Approved"),
      Seq[Any]("31".toInt,"Barrett","Amies","747.61".toDouble,"Finished"),
      Seq[Any]("14".toInt,"Dallas","Davoren","383.14".toDouble,"Started"),
      Seq[Any]("98".toInt,"Twyla","Coppledike","283.1".toDouble,"Finished"),
      Seq[Any]("29".toInt,"Casey","McFater","825.13".toDouble,"Started"),
      Seq[Any]("45".toInt,"Leigha","Yurikov","242.69".toDouble,"Approved"),
      Seq[Any]("14".toInt,"Dallas","Davoren","218.55".toDouble,"Started"),
      Seq[Any]("42".toInt,"Heywood","Stork","563.05".toDouble,"Finished"),
      Seq[Any]("65".toInt,"Evelyn","Goulbourne","556.41".toDouble,"Approved"),
      Seq[Any]("8".toInt,"Sibley","Reasun","269.46".toDouble,"Finished"),
      Seq[Any]("1".toInt,"Griz","Roubeix","250.99".toDouble,"Approved"),
      Seq[Any]("61".toInt,"Baryram","Ades","745.73".toDouble,"Started"),
      Seq[Any]("92".toInt,"Lewes","Kennan","586.83".toDouble,"Finished"),
      Seq[Any]("71".toInt,"Uriel","Iacivelli","259.29".toDouble,"Finished"),
      Seq[Any]("93".toInt,"Earl","Colenutt","778.37".toDouble,"Approved"),
      Seq[Any]("61".toInt,"Baryram","Ades","237.5".toDouble,"Started"),
      Seq[Any]("81".toInt,"Leigh","Mackett","598.37".toDouble,"Started"),
      Seq[Any]("35".toInt,"Viva","Schulke","372.04".toDouble,"Finished"),
      Seq[Any]("83".toInt,"Tish","Mankor","809.84".toDouble,"Pending"),
      Seq[Any]("13".toInt,"Violet","Alston","877.45".toDouble,"Finished"),
      Seq[Any]("77".toInt,"Deeanne","Kaye","776.76".toDouble,"Started"),
      Seq[Any]("77".toInt,"Deeanne","Kaye","843.36".toDouble,"Started"),
      Seq[Any]("64".toInt,"Aguistin","Lipman","71.47".toDouble,"Started"),
      Seq[Any]("25".toInt,"Alyssa","Mance","305.39".toDouble,"Started"),
      Seq[Any]("48".toInt,"Queenie","Basire","696.56".toDouble,"Finished")
    ))


    val dfOutComputed = graph.With_FullName(spark, dfIn)
    

    assertPredicates(
      "out",
      dfOutComputed,
      Seq(
        col("order_status").isin("Finished", "Started", "Approved", "Pending")
      ) zip Seq (
        "order_status"
      )
    )
}

  def inDf(columns: Seq[String], values: Seq[Seq[Any]]): DataFrame = {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    // defaults for each column
    val defaults = Map[String, Any](
      "customer_id" -> 0,
      "first_name" -> "",
      "last_name" -> "",
      "amount" -> 0.0,
      "order_status" -> ""
    )

    // column types for each column
    val columnToTypeMap = Map[String, DataType](
      "customer_id" -> IntegerType,
      "first_name" -> StringType,
      "last_name" -> StringType,
      "amount" -> DoubleType,
      "order_status" -> StringType
    )

    createDF(spark, columns, values, defaults, columnToTypeMap, "in")
  }

  def outDf(columns: Seq[String], values: Seq[Seq[Any]]): DataFrame = {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    // defaults for each column
    val defaults = Map[String, Any](
      "customer_id" -> "",
      "first_name" -> "",
      "last_name" -> "",
      "amount" -> "",
      "full_name" -> "",
      "order_status" -> ""
    )

    // column types for each column
    val columnToTypeMap = Map[String, DataType](
      "customer_id" -> StringType,
      "first_name" -> StringType,
      "last_name" -> StringType,
      "amount" -> StringType,
      "full_name" -> StringType,
      "order_status" -> StringType
    )

    createDF(spark, columns, values, defaults, columnToTypeMap, "out")
  }

  def assertPredicates(port: String, df: DataFrame, predicates: Seq[(Column, String)]): Unit = {
    predicates.foreach({
      case (pred, name) =>
        Assert.assertEquals(
          s"Predicate $name [[`$pred`]] not universally true for port $port",
          df.filter(pred).count(),
          df.count()
        )
    })
  }
}
