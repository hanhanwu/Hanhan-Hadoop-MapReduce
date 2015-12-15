# Hadoop-MapReduce
This folder contains Hanhan's Hadoop MapReduce Java code



WordCountImproved.java

The mapper in this file is used to give each word count 1

Hadoop has built-in LongSumReducer to add word count

Meanwhile, this java code has solved the problems caused by Unicode encoding by using java.text.Normalizer



WikipediaPopular.java

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



RedditAverage.java, LongPairWritable.java

Each Reddit Comment has a score, each record is represented in a JSON format. This file works on determing the average of each subreddit.

Sample input:

{"archived": true, "author": "xelfer", "author_flair_css_class": null, "author_flair_text": null, "body": "Now with new xkcd subreddit!", "controversiality": 0, "created_utc": "1201240417", "distinguished": null, "downs": 0, "edited": false, "gilded": 0, "id": "c02zpp2", "link_id": "t3_66k1x", "name": "t1_c02zpp2", "parent_id": "t3_66k1x", "retrieved_on": 1425824816, "score": 2, "score_hidden": false, "subreddit": "xkcd", "subreddit_id": "t5_2qh0z", "ups": 2}

The mapper will record the (comment number, score) for each subreddit

The reducer will calculate the average for each subreddit

The combiner will combine the output from the mapper so that records with the same key will be combined into 1 record. This minimizes the amount of data that hits the expensive shuffle



MultiLineRecordReader.java

This one do the same thing as RedditAverage.java but deals with multiple line of JSON



EulerEstimator.java

We can estimate Euler's consant, e using a stochastic representation. Given a sequence X1,X2,… of uniform random variables on the range [0,1], we can take the smallest number of these such that the total is greater than one:

V=min{n∣X1+X2+⋯+Xn>1}

The expected value E(V)=e

This file is using mapreduce to calculate e. The input files are files that contain a single integer on each line (use the default TextInputFormat). The integer indicates the number of iterations for your main loop.

The mapper repeats the estimation of the random variable the number of times indicated in the input.

The Hadoop counters will count the total number of iterations you have make, and the number of random numbers you had to generate.
