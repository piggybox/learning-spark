

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.joda.time.DateTime

import scala.Option

object Exercise {
    def main(args: Array[String]) {

      // Spark config/context setup
      val conf = new SparkConf().setAppName("radius-exercise")
      conf.set("spark.hadoop.validateOutputSpecs", "false") // Overwrite output
      val sc = new SparkContext(conf)


      // Do Not Call list
      val donotcall = sc.textFile(args(0)).map(phone => (phone, 1))  // Add a flag


      // Transaction processing
      val transaction = sc.textFile(args(1)).map { line =>
        val fields = line.split(";")
        // userid, spending, year with simple conversion
        (fields(0), fields(1).substring(1).toFloat, new DateTime(fields(2)).getYear)
      }


      // Most valuable customers
      val topUsers = transaction.filter(t => t._3 == 2015) // Only in 2015
        .map(t => (t._1, t._2)) // userid, spending
        .reduceByKey( _ + _ ) // Calculate sum
        .sortBy(t => t._2, ascending=false)
        .take(1000) // Get top 1000


      // Customer processing
      val users = sc.textFile(args(2)).map{ line => line.split(";") }


      // Ignore those without phones as this is a phone campaign
      val usersWithPhone = users.filter(list => list.size == 4)
        .flatMap(l => l(3).split(",") // Split on phones
        // => (phone, (userid, name)), drop emails as the final answer doesn't require that
        .map(phone => (phone, (l(0), l(1)))))


      // Filter out those in the Do Not Call list
      val callableUsers = usersWithPhone.leftOuterJoin(donotcall)
        .filter(t => t._2._2.isEmpty)  // => (phone, ((userid, name), None))

        // Merge phones back to original form
        .map(t => (t._2._1, t._1))
        .reduceByKey( _ + "," + _ )


      // Put together
      val topUsersCallList = callableUsers.map(t => (t._1._1, (t._1._2, t._2))) // => (userid, (name, phones))
        .join(sc.parallelize(topUsers)) // Convert back to RDD
        .map(t => s"${t._1};${t._2._1._1};${t._2._1._2};${t._2._2}") // pretty ugly ><

      // I choose to follow the format of original input by using ';' as delimiter
      // because ',' is already used in the phone list, though it looks like the description
      // of the output is asking me to use ',' as delimiter

      // The result isn't required to be ordered by money spent, though probably a good idea

      // Output: userid, name, phone list, money spent
      val output = args(3)
      topUsersCallList.saveAsTextFile(output)
    }
}
