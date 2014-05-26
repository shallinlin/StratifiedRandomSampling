# Stratified Random Sampling
## The Alogorithm
The algorithm is based on the paper "Scalable Simple Random Sampling and Stratified Sampling" (ScaSRS algorithm) by Xiangrui Meng
This algorithm works effectively on large-scale data sets. When I test on small data set we almost accept  all the data based on the crietrion from the paper. 
## Spark exercise
The Stratified Sampling is count based sampling that allocates different sample size for different stratas.
The strata can be defined using function to append indicator for strata with data RDD. Here I developed "myAppendIndicator" function as an example.
The basic steps for Stratified Random Sampling is:
* Compute collection of Strata IDs
* Filter out each Strata
* Compute Strata size and corresponding sample size for each strata
* ScaSRS for each strata (RDD filtered out from entire dataset)
  * Assign Random value for each item in Strata
  * Compute threshold for acception and rejection at flight
  * Filter out the waiting list RDD
  * Random Sorting based SRS method for waiting list
  * Construct sampled RDD by taking Union of the accepted RDD and sampled RDD from waiting list
* Construct sampled RDD for entire dataset, by taking Union of the sampled RDD of each strata

## Working code with Spark-shell
Here the file "databrick1.scala" is a working code on Spark-shell. It demonstrates the usage of developed "myScaSRS(RDD,sampleSize)" function which is Serializable.

## StandAlone Scala with Spark API
The code was modified briefly modified for standalone scala app. "databrick2.scala" is added.

## Trait for ScaSRS with Spark API
The code was further modified with extended trait. 
The "RandomStratifiedSampler" extends from "RandomSampler" and with trait "StratifiedSampler" for methods including appending trata IDs, ScaSRS for each strata and overrided function of "sample" to sample Strata-by-Strata and combine with Union.


