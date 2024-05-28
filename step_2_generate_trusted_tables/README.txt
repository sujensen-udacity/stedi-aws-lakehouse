
In this step, I use Glue jobs to created the trusted tables.

The Python scripts in this directory are downloaded from Glue.  This is the Glue-generated script which represents the job I built in Glue Studio.  There are three jobs.

(1) For the customer_landing table, to create the customer_trusted table, I filter out any customers who do not want their data shared for research.  This left 482 records.

(2) For the accelerometer landing table, to create the accelerometer_trusted table, I joined on the customer_trusted table in order to only keep accelerometer records from customers who agreed to share their data for research.  This led to 40981 records in the accelerometer_trusted table.

The code / screenshots for the step_trainer_trusted table is in the next directory ("step_3"), as it was dependent on customer_curated.



