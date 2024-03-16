# SF Fire Incidents Data Pipeline


In this blog post, I will share my experience of working on a data analysis project using PySpark and Databricks. The project involved exploring and visualizing a large dataset of fire department calls for service in San Francisco. I will explain the steps I took to set up the cluster, load the data, perform some basic transformations and aggregations, and create various plots to answer some interesting questions.

# Dataset:

The dataset I used is available on the San Francisco Open Data Portal ([https://data.sfgov.org/Public-Safety/Fire-Department-Calls-for-Service/nuek-vuh3](https://data.sfgov.org/Public-Safety/Fire-Department-Calls-for-Service/nuek-vuh3)). It contains 6.02 million rows and 34 columns, where each row represents a response to a fire call with geographical and temporal information. The dataset covers the period from January 2000 to December 2020.

# Technologies used:

To work with this dataset, I used `PySpark`, which is a `Python API` for `Apache Spark`, a distributed computing framework that allows processing large-scale data in parallel. PySpark provides an interface to manipulate resilient distributed datasets (`RDD`s), which are collections of elements that can be operated on in parallel across multiple nodes in a cluster.

To set up the cluster, I used `Databricks`, which is a cloud-based platform that provides an interactive workspace for data science and engineering. Databricks allows creating and managing clusters of virtual machines that run Spark and other tools. Databricks also enables collaboration and sharing of notebooks, which are documents that combine code, text, and visualizations.

I configured my cluster to have the following settings:

- Cluster mode: Multi-node
- Isolation level: No isolation shared
- Runtime version: 5.0
- Worker/Driver type: e2-highmem-2
- Min workers: 1
- Max workers: 8 with autoscaling

To improve efficiency, I also configured the cluster to have spark.executor.memory being 4g and .persist(StorageLevel.MEMORY_ONLY) intermediate RDDs that were used often.

To load the data, I used `Google Cloud Platform (GCP)` buckets to store the CSV file of the dataset. I then created a PySpark RDD by reading the file from the bucket using sc.textFile() method. I also parsed the header and split each line by comma to get an RDD of lists.

To perform some basic transformations and aggregations, I used PySpark RDD methods such as map(), filter(), reduceByKey(), sortByKey(), etc. For example, to get the number of alarms per call type group, I mapped each list to a tuple of (call_type_group, 1), filtered out the null values, reduced by key to get the counts, and sorted by key to get an ordered list.

To create various plots to answer some interesting questions, I used matplotlib library in Python. I also converted some RDDs to pandas dataframes using toPandas() method for easier plotting. For example, to display a pie chart representing the calls per season, I first created a function to map each date to a season based on the month. I then applied this function to the date column of the dataframe using apply() method. I then grouped by season and counted the number of calls using groupby() and count() methods. I then plotted the resulting dataframe using plotly method.

Some of the questions I answered with plots are:

- What are the most common call type groups?
- How many zip codes are there for each final call disposition?
- How does the distribution of daily counts look like?
- What is the trend of counts of calls per year?

Here are some examples of the plots I generated:

I used `plotly library` to plot interactive plots

![newplot.png](SF%20Fire%20Incidents%20Data%20Pipeline%2080fae4bff89b41d4825dff3961f68952/newplot.png)

![output1.png](SF%20Fire%20Incidents%20Data%20Pipeline%2080fae4bff89b41d4825dff3961f68952/output1.png)

![newplot1.png](SF%20Fire%20Incidents%20Data%20Pipeline%2080fae4bff89b41d4825dff3961f68952/newplot1.png)
