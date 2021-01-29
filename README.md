The purpose of this data lake in the context of Sparkify is to transform their massive cloud dataset into a usable analytical structure.
Their data currently resides in S3, therefore an ETL pipeline has been built to load and transform this data into a star schema of Parquet files
which can be utilized by their analytical team.
This schema is more suited to the greater size of the dataset that Sparkify must now work with moving into the future.

As the data resides in S3, in directories of JSON logs and metadata, I have developed an ETL pipeline to load and transform this data into a star schema
The Parquet files can now be used by the analytics team to gather further insight into the Sparkify dataset.
   
File Descriptions:
 - dl.cfg: Contains access credentials for the AWS data lake.
 - etl.py: Contains code to run etl process.

Run Instructions:

1. Launch a new Python3 launcher (console)
2. run etl.py
