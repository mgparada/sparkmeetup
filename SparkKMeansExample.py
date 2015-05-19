from pyspark.sql import	SQLContext
from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF	
from numpy import array
from pyspark.mllib.clustering import KMeans
import sys	

def show(x):	
  print	x	


if len(sys.argv) != 2:
	print >> sys.stderr, "Usage: handsOn4.py <filename>"
	exit(-1)  
sc= SparkContext()
sqlContext=SQLContext(sc)	 
tweets=sqlContext.jsonFile(sys.argv[1])	 
tweets.foreach(show)	 
tweets.registerTempTable("pt")	 
words=sqlContext.sql("select hashtags from pt where hashtags is not null")	
words.foreach(show)	
  

wordsArray=words.map(lambda x:array(x[0]))	
hashingTF=HashingTF()	
tf=hashingTF.transform(wordsArray)	
tf.foreach(show)
show("Executing Kmeans")
clusters = KMeans.train(tf,2,1,1)
results= wordsArray.map(lambda x:array([x,clusters.predict(hashingTF.transform(x))]))	
results.foreach(show)	
  
