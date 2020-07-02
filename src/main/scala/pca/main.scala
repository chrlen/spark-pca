package pca
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix,MatrixEntry,CoordinateMatrix}
import org.apache.spark.sql.Row
import scala.math.sqrt
import org.apache.spark.rdd.RDD

// Execute with the following command
//spark-submit scalainference_2.11-1.0.jar --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:MaxDirectMemorySize=32768m"

// Define a simple Aggregator-Class that consumes Values of the type Double and computes the mean.
class Agg{
  var sum:Double = 0
  var count: Int = 0
  def apply(value : Double) ={
    sum += value
    count +=1
  }
  def getMean(): Double = sum / count
}

object main{

  // Create a Mean-Aggregator, apply it to every value of the given Array and return the Mean
  def myMean(arr : Iterable[MatrixEntry]):Double ={
    val aggregator = new Agg()
    arr.foreach{
      case entry => aggregator(entry.value)
    }
    aggregator.getMean()
  }

  def main(args:Array[String]):Unit = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("BigData")

    // Set spark paramters to enable processing of large dataset
    conf.set("spark.yarn.executor.memoryOverhead","8192")
    conf.set("spark.buffer.pageSize","2M")
    conf.set("spark.buffer.pageSize","2M")
    conf.set("spark.sql.shuffle.partitions","500")
    conf.set("spark.executor.memory","32G")
    conf.set("spark.driver.memory","16G")
    conf.set("spark.dynamicAllocation.enabled",true)
    conf.set("spark.kryoserializer.buffer.max","512")

    //does not apply in client mode
    //conf.set("spark.yarn.driver.memoryOverhead","16384")

    val sc = new SparkContext(conf)
    log.info("Start Loading Training Images")

    // Due to restrictions on version of spark on the cluster the images had to be converted to csv-files
    // No library for processing and loading of images was available
    val trainingPictures = sc.wholeTextFiles("facesInTheWildCsv")

    // Load content from the texf-files and cache in RAM for further processing
    val trainingPicturesContent = trainingPictures.map { case (filename, content) => content}.cache

    log.info("Computing Matrix entry triples")

    // build a list of Matrix-Entries from the cached csv-file contents and cache them
    val trainingPicturesEntries = trainingPicturesContent.
      map(_.split("[,\n]")).
      zipWithIndex.flatMap{
      case (arr,rowIndex) => arr.map(_.toDouble).zipWithIndex.map{
        case (value,colIndex) => MatrixEntry(rowIndex.toInt,colIndex,value) }
    }.cache

    // Print some statistics to observe correct execution on the cluster
    log.info("Entries computed: " + trainingPicturesEntries.count())
    log.info("Computing Number Of Images")
    val numOfTrainingImages = trainingPicturesEntries.filter( x => (x.j == 0) ).count()
    log.info("Number of Images: " + numOfTrainingImages)
    log.info("Computing Number Of Columns")
    val imageSize = trainingPicturesEntries.filter( x => (x.i == 0) ).count()
    log.info("Number of Columns: " + imageSize)

    val blockSize = 16

    //val designMatrix = new CoordinateMatrix(trainingPicturesEntries,numOfTrainingImages,imageSize).toBlockMatrix(blockSize,blockSize).cache
    //designMatrix.validate
    //log.info("Design-Matrix: (" + designMatrix.numRows() + ", " + designMatrix.numCols() + ")")

    // Compute the column-means for the translation that is part of the pca-rotation
    // Group by column-index and reduce the groups to mean with the myMean function
    log.info("Calculate Column Means")
    val colMeans = trainingPicturesEntries.groupBy{
      case entry => entry.j
    }.map{
      case (key,it) => myMean(it)
    }.collect()

    //Center the Training-Data by subtracting the column-means
    log.info("Calculate centered Design matrix entries")
    val meanFreeTrainingPictureEntries = trainingPicturesEntries.map(entry => new MatrixEntry(entry.i,entry.j,entry.value - colMeans(entry.j.toInt)))

    // Construct the design-Matrix from the centered Entries
    val designMatrix = new CoordinateMatrix(meanFreeTrainingPictureEntries,numOfTrainingImages,imageSize).toBlockMatrix(blockSize,blockSize).cache
    // Use the builtin function to validate the structure of the matrix
    designMatrix.validate
    log.info("Design-Matrix: (" + designMatrix.numRows() + ", " + designMatrix.numCols() + ")")

    // Compute the covariance as the product of the design-matrix with itself (transpose)
    val cov = designMatrix.transpose.multiply(designMatrix)
    log.info("Start SVD")

    // Compute the eigenvectors of the covariance matrix by singular value decomposition and save to file
    val componentsMatrix = cov.toIndexedRowMatrix.computeSVD(imageSize.toInt,true).U.toBlockMatrix.cache
    log.info("Save U")
    log.info("Components-Matrix: ",componentsMatrix.numRows(),componentsMatrix.numRows())
    sc.parallelize(componentsMatrix.transpose.toLocalMatrix.toArray).saveAsTextFile("componentsMatrix")

  }
}


