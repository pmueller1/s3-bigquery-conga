# s3-bigquery-conga

## Piping AWS EC2/S3 files into BigQuery using Lambda and python-pandas##

I'm putting this file out into the ether to help AWS customers experience Google's BigQuery.    
You can think of this as a pipeline that looks like so:

CSV_File_on_EC2 --> S3 --> ObjectNotification --> AWS LAMBDA --> BQ Streaming Inserts --> BQ_Tables_organized_by_day

In other words, get your CSV files onto S3 (however you like) and have them show up in BigQuery moments later.

There are a few key concepts you need to know about to get this snippet working.  You need to understand and setup:

- How to push data from your EC2 instances (or wherever) into S3 using a tool like 's3cmd' or 'aws s3'.
- How to use S3's object notification, and [how to prime it to tell Lambda](http://docs.aws.amazon.com/lambda/latest/dg/with-s3.html) about any new files arriving in an S3 bucket.  
- How to use python's 'virtualenv' command, so that you can create a self-contained zip file, structured the right way to include all the imported modules (like Pandas!) that we use here.  A good overview can be found [here](http://www.perrygeo.com/running-python-with-compiled-code-on-aws-lambda.html).
- How to deploy a Lambda function, and make sure it runs with a role suitable to read from your S3 bucket. (Alternately, how to just insert your AWS credentials so boto can use them directly.)
- If you've used BigQuery before, you'll recognize that you need to change the attached to point to your correct <project_id, dataset_name, table_name and service_key>.  Obviously, your service key, in json format, gets packaged up in the root directory of your zip file.  You'll forgive me for not posting our own!
- Don't skimp on timeout or RAM when it comes Lambda.  It's dirt cheap, and you can always scale back later, once you're seeing successful completions in your Cloudwatch logs.
- The essence of this whole project can be found in the 'stream-to-bq.py' file.  If you've used Lambda for Python before, you could probably just get started by adapting that file to your needs.  

Fortunately for us, Python-Pandas has done the heavy lifting of writing a '.to_bq' extension to a dataframe for BigQuery-streaming.  There are other approaches, including doing this with Kinesis or PubSub, as well as using something like fluentd to publish directly to BigQuery, but this approach is elegant for people who are a) used to python-pandas and b) prefer to keep things most simple on the instance end:  ie. only have to push to S3 to get the ball rolling.  

Since we're using Pandas here, you could also manipulate your dataframe however you like before sending the results on up to BigQuery.

I previously [wrote a post](http://www.atso.com/our-bq-trip1/) outlining why we went this route in the first place.  

Comments welcome! 

Twitter: @pmueller


Credits: Christopher Corus - ATS, Chris Brokes - ATS, Abhijit Chanda - Gamma, Felipe Hoffa - Google, Graham Polley - Shine 
