# spotify-ETL-Snowflake
Data extraction from the Spotify API, loads raw data into AWS S3 using AWS Lambda, and transforms it for analysis. A data warehouse is created on Snowflake, and Snowpipe is used to automatically ingest the transformed data, enabling efficient and scalable data analysis.

![Screenshot (112)](https://github.com/user-attachments/assets/ecbb1624-7ea3-49aa-9049-f318039b1924)

Steps we will follow :
Extarcting the spotify data using Spotify API which is free to use API and we will using it to create a database of top 100 songs, which will change with time and thus creating a real time data streaming.
First, we will run our code on jupyter notebook when we will be extracting the data and then implementing lambda function.
Loading the raw data into S3 bucket and further transfromation is applied using Lambda.
The transformed data is also stroed in S3 bucket.
Creating a Snowpipe on S3 bucket for creating a warehouse for analysis.
And finally we will create a trigger which we can specify the duration and it will run our Lambda function acutomaticalluy after some certain amount of time to get new and fresh data.
