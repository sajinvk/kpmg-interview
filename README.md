Programming Language : Python 3.7 
etl_main.ipynb : The executable file 

ETL :




Actual Python ocde 

Python Package Module :

To run the code : python etl_main.py 
pre-requisite :  Configuration settings - config_data.json in the same directory . Data source details and output format is set in the configuration file.

Scripts :

etl_main.py : The main executable file 
logging_class.py : Logs output to file etl_log.out in the directory of execution 
get_data.py 
transformation.py  
load_output.py 
config_data.json : Configuration file for source data and output type 

Functional Flow :

etl_main.py - Execute this file 
get_data.py : Loads input file to pandas data frame as passes back to main class 
transformation.py :  
  Convert Unix time column in to a human readable format. 
  Extract medium ,source and path from URL.
  Output : Returns 3 data frame (summary data ) to the main class 
  Top count medium and source.
  Distinct users count, min time , max time for a subset of records.
  Distinct users count by day. 
load_output.py : Generate output in the form of CSV , JSON or Parquet file for the summary data


Pyspark Script :

Problem description : When the Source data volume increases , panda struggles to keep the dataframe in memory .
Alternative solution :
Split source file to smaller size (not practical and is not scalable)
Use distributed data framework.

Script name : pyspark-utm.ipynb
Read data using spark.read and convert to parquet 	. The file gets split using automatic partition 
Using spark sql to perform transformation.

Test metrics : ( Single server in a cluster)
Size reduced by 60% prior compression 
Massive parallel operation benefits . The transformation time < 10% of actual time taken compared to single python script.
Spark SQL offers easy transformation 

Visual of the summary data in power bi file . 
