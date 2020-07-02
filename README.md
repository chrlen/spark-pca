

This repository contains an implementation of the principal component analysis in scala and spark.
The PCA was part of an easy face-detector trained on the [faces in the wild](http://vis-www.cs.umass.edu/lfw/) dataset.
This implementation was part of a lecture in big-data analytics where a final project with free choice of the topic and used programming languages was mandatory.
The implementation was constrained to run on the university cluster running the cloudera distribution of Spark in the version 1.6.
This version was old at the time of the project and did not provide any functions to load and decode images.
Therefore, the images where converted to grayscale and stored as csv-files with a python script and then loaded as text-files.

Unfortunately compilation is not possible anymore are the sources for spark 1.6 are not provided anymore through sbt as they where at time of the project.

Check the file [main.scala](src/main/scala/pca/main.scala) for the implementation.