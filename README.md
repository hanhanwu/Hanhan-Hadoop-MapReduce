# Hadoop-MapReduce
This folder contains Hanhan's Hadoop MapReduce Java code

1. WordCountImproved.java

The mapper in this file is used to give each word count 1

Hadoop has built-in LongSumReducer to add word count

Meanwhile, this java code has solved the problems caused by Unicode encoding by using java.text.Normalizer

