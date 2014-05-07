crunch.TS
=========

The goal of Crunch.TS is to implement reusable data structures and reusable analysis algorithms for several "standard & advanced time series" analysis tasks. The work is based on Hadoop.TS which (http://www.ijcaonline.org/archives/volume74/number17/12974-0233) is also on Guthub.

There are at least two groups or types of time series: "continuos equidistant time series" and "event time series". 
Both can be converted into each other (if certain conditions are met), but therefore some metadata is required.

Especially for correlation analysis and fluctuation analysis one should to do some consistency checks before the 
expensive calculations are started. In order make such operations fast and reliable, Crunch.TS implements generic 
time series representations for both types of time series, as well a the conversion functions as Crunch DoFn. 

Univariate time series analysis is done on individual time series representing a property of one object. From 
multivariate time series analysis algorithms one can get correlation- and dependency-networks. Such algorithms
can also be seen as a kind of graph-generators which transform a time series bucket into a network layer.

Another use case is "sessionization" of weblogs or creation of "event series" from data, collected via Flume. 
Our generic data structures should be used to direct the event flow and to create time series buckets. This is
a very first step towards advanced time series analysis topics. 

This project contributes the following algorithms: 

* Detrendet Fluctuation Analysis (DFA)
* Return-Intervall-Statistics (RIS)
* Cross-Correlation Analysis (CC) for a) correlation and b) dependency networks.

to Apache Crunch. Using the Spark-, in Memory or MapReduce-Pipeline, one
can do several analysis steps on a Workstation or in a large cluster.
 


 
