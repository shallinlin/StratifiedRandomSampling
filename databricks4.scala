//Example to extend the function to Trait
//Example of the usage myScaSRS Function in stand-alone spark App with Spark API
//- Ramdomly 5% sampling from 125K lines from Shakespear's works
//- Two stratas for with and without the word "this"
//@ Xiaolin Lin, May 25th, 2014

package org.apache.spark.util.random

import java.util.Random

import cern.jet.random.Poisson
import cern.jet.random.engine.DRand

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 *
 * @tparam T item type
 * @tparam U sampled item type
 */
@DeveloperApi
trait RandomSampler[T, U] extends Pseudorandom with Cloneable with Serializable {

  /** take a random sample */
  def sample(items: Iterator[T]): Iterator[U]

  override def clone: RandomSampler[T, U] =
    throw new NotImplementedError("clone() is not implemented.")
}


/**
 * :: DeveloperApi ::
 * Trait example for Stratified Sampling
 * Features added for multiple strata
 * Function "myAppendIndicator" to append indicator int value for Strata IDs
 * Function "myScaSRS" process ScaSRS for single Strata
 * Field "mapStrataInfor" process and update Strata information for sampling
 * It is possible to change the sampled item type. 
 * Sampled each Strata with sampled size proportion to Strata count
 * @param dataStrata sample
 * @param sampleSize 
 * @tparam T item type
 */
@DeveloperApi
trait StratifiedSampler[T, U] {
	def myAppendIndicator(dataStrata: Iterator[T]): (Iterator[(U,Int)])
	def myScaSRS(dataStrata: Iterator[T], sampleSize: Long) : Iterator[U]
}


/**
 * :: DeveloperApi ::
 * A Scalable Stratified Random Sampler
 * based on random Sampler and with trait StratifiedSampler
 * It is possible to change the sampled item type. 
 * Sampled each Strata with sampled size proportion to Strata count
 * @param sampleRatio, double, ratio of Stratified Sampling for entire dataset
 * @param minSampleNumber, Int, minimize sample size fo each strata, 1 for common case
 * @tparam T item type
 */
@DeveloperApi
class RandomStratifiedSampler[T](sampleRatio: Double, minSampleNumber: Int)
    (implicit random: Random = new XORShiftRandom)
  extends RandomSampler[T, T] with StratifiedSampler[T,T] {

  def this(ratio: Double)(implicit random: Random = new XORShiftRandom)
    = this(0.0d, ratio)(random)

  override def setSeed(seed: Long) = random.setSeed(seed)

//Function to append strata ID after each item, can be override for specific label assigning
//here we leave an example, the function should be override when create a Sampler
// in order to specify different strata
//
//  override def myAppendIndicator(dataStrata: Iterator[T]): (Iterator[(T,Int)]) {
// 	dataStrata.mapPartitionsWithIndex{ (indx, iter) =>
//  	  iter.map(x => (x, if (x.contains("this")) 1 else 0))
//    }
//  }
  
//define function myScaSRS, here we input sample size for example
  override def myScaSRS(dataStrata: Iterator[T], sampleSize: Long) : Iterator[T] = {
	//configure
	//val sc = new SparkContext("local", "Simple App", "/Users/Shallin/Documents/spark-0.9.1",
    //    List("target/scala-2.10/simple-project_2.10-1.0.jar"))

    //assign random value to each item
    val assignStrata = dataStrata.mapPartitionsWithIndex{ (indx, iter) =>
    val rand = new scala.util.Random(indx+1234)
    	iter.map(x => (x, rand.nextDouble))
    }

    //compute the sample ratio, upper and lower threshold for the starta
    val sampleRatio = sampleSize.toDouble / dataStrata.count
    val p = sampleRatio
    val delta = 5*1e-5 //refer to paper from Meng's
    val n = dataStrata.count()
    val gamma1 = -scala.math.log(delta)/n
    val q1 = scala.math.min(1,p+gamma1+scala.math.sqrt(gamma1*gamma1+2*gamma1*p))
    val gamma2 = -2*scala.math.log(delta)/(3*n)
    val q2 = scala.math.max(0,p+gamma2-scala.math.sqrt(gamma2*gamma2+3*gamma2*p))
    //extreme case: with small n, q1 and q2 will be the boundary value, 
    //thus the entire data will be processed using SRS
	
    //item with value below threshold 1 is accepted, put into accepted RDD
    val acceptedStrata = assignStrata.filter(line => line._2 < q2).map(item => item._1)
	
    //item with value above threshold 2 is rejected
    val rejectedStrata = assignStrata.filter(line => line._2 >= q1)
	
    //item with value in between is put into process RDD
    val waitingStrata = assignStrata.filter(line => (line._2 < q1 && line._2 >= q2)) 
	
    //simple random sampling (update sample number needed) for process RDD, put into SRS RDD
    val waitingCount = scala.math.ceil(scala.math.max(0,
    	sampleSize - acceptedStrata.count())).toInt
    val processStrata = sc.parallelize(waitingStrata.map(item => item.swap).
    	sortByKey().take(waitingCount)).map(item => item._2)
		
    //union accepted RDD with SRS RDD, put into sampled RDD 1.
    acceptedStrata.union(processStrata)
  }
  
  override def sample(dataAll: Iterator[T]): Iterator[T] = {
  	 //create RDD with indicator
    val indicatorStrata = myAppendIndicator(dataAll)

	//find label and count of stratas, equalling the max strata ID +1
	//assuming the first group starting as label "0"
	val numStrata = indicatorStrata.map(x=>x._2).reduce((a, b) => Math.max(a, b))+1
	val mapStrataInfo = indicatorStrata.map(x=>x.swap).countByKey
	
	//init the output RDD
	var sampledAll = sc.parallelize(dataAll.take(1))

	//for each strata, process on strata No. idStrata
	for ((key, value) <- mapStrataInfo){

	  //get Strata ID and size
	  var idStrata = key
	  val currentStrataSize = value
	  
	  //use filter to process Random Sampling on each Strata
	  val currentStrata = indicatorStrata.filter(x => (x._2==idStrata)).map(x=>x._1)
  
	  //minimum sample number to 1 if the strata is not empty
	  val currentSampleSize = scala.math.max(scala.math.min(minSampleNumber,currentStrataSize)
	    ,scala.math.round(sampleRatio * currentStrataSize))
		
	  println("Sample Strata #" + idStrata.toString + " count " + currentStrataSize +
		 " sample " + currentSampleSize)

	  //ScaSRS for current Strata
	  val currentSampledStrata = myScaSRS(currentStrata,currentSampleSize)

	  //union sampled RDD for each strata, create final sample	
	  if (idStrata == 0){
    	sampledAll = currentSampledStrata
	  }else{
		sampledAll.union(currentSampledStrata)
	  }
	}
	sampledAll
  }
}  
  









