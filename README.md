# Hadoop-MapReduce
This folder contains Hanhan's Hadoop MapReduce Java code

1. WordCountImproved.java

The mapper in this file is used to give each word count 1

Hadoop has built-in LongSumReducer to add word count

Meanwhile, this java code has solved the problems caused by Unicode encoding by using java.text.Normalizer


2. WikipediaPopular.java

Wikipedia publishes page view statistics here: https://dumps.wikimedia.org/other/pagecounts-raw/

Create a MapReduce class WikipediaPopular that finds the number of times the most-visited page was visited each hour. Here, only English Wikipedia pages will be reported, Main page or special pages are boring and will not be reported.

Here is a sample input:

De Facialisparese 1 56557

af Faer%C3%B6ereilande 1 219519

af Fahrenheit 1 39001

af Faith_Like_Potatoes 1 8584

af Falkland-eilande 1 22766

af Familie_(biologie) 1 14376

af Familie_(verwantskap) 1 54979

af Faro%C3%ABes 1 15934

af Fat_Man 1 10044





