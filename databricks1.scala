//Example of the usage myScaSRS Function in Spark-shell codes
//- Ramdomly 5% sampling from 125K lines from Shakespear's works
//- Support RDD with indicator tuple for different stratas
//- The example here sample two stratas for lines with and without the word "this"
//@ Xiaolin Lin, May 25th, 2014

/****************/
//support functions

//example to create RDD with strata indicator
def myAppendIndicator(dataStrata: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[(String, Int)] = {
  //append list of indicator for strata ID to each item of RDD
  dataStrata.mapPartitionsWithIndex{ (indx, iter) =>
  	iter.map(x => (x, if (x.contains("this")) 1 else 0))
  }
}

//define function myScaSRS, here we input sample size for example
def myScaSRS(dataStrata: org.apache.spark.rdd.RDD[String], sampleSize: Long) : org.apache.spark.rdd.RDD[String] = {
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
  val waitingStrata = assignStrata.filter(line => (line._2 < q1 && line._2 >= q2)) //**
	
  //simple random sampling (update sample number needed) for process RDD, put into SRS RDD
  val waitingCount = scala.math.ceil(scala.math.max(0, sampleSize - acceptedStrata.count())).toInt
  val processStrata = sc.parallelize(waitingStrata.map(item => item.swap).sortByKey().take(waitingCount)).map(item => item._2)
		
  //union accepted RDD with SRS RDD, put into sampled RDD 1.
  acceptedStrata.union(processStrata)
}

/******************/
//main code

//create RDD from pg100.txt
val dataAll = sc.textFile("pg100.txt")
val sampleRatio = 0.05
val minSampleNumber = 1
println("Sample " + (sampleRatio*100).toString() + 
	"% lines with and without 'this' from Shakespear's works");

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
		
  println("Sample Strata #" + idStrata.toString + 
    " count " + currentStrataSize + " sample " + currentSampleSize)

  //ScaSRS for current Strata
  val currentSampledStrata = myScaSRS(currentStrata,currentSampleSize)

  //union sampled RDD for each strata, create final sample	
  if (idStrata == 0){
    sampledAll = currentSampledStrata
  }else{
	sampledAll.union(currentSampledStrata)
  }
}


//output
println("Sample from " + numStrata + " Stratas. " + 
		" Count " + dataAll.count + " sample " + sampledAll.count)
sampledAll.collect.foreach(x=> println(x))

