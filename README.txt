
This project builds an AWS lakehouse for the STEDI project researchers.  

To run this code and re-create the Lakehouse:

1) Update SETUP.sh with your own AWS configuration variables (e.g. VPC ID, bucket name, etc.).  Running SETUP.sh will create the necessary AWS Glue components, as well as load the starter data from the project repo into the S3 bucket.

2) See the code and screenshots in "step_1_define_and_inspect_landing_tables".  We must define Glue tables for the raw data loaded into our S3 bucket.  There are three "landing" tables at this level of the lakehouse:  customer_landing, accelerometer_landing, and step_trainer_landing.  These tables are raw, contain known faulty data (fulfillment center serial numbers for customers that haven't received a step sensor yet), and PII.

3) Then, see the code and screenshots in "step_2_generate_trusted_tables".  This includes the code and screenshots from creating the customer_landing and accelerometer_landing tables.  These tables do not include information from customers who did NOT want their data shared with researchers.  These tables do still include PII and are not appropriate for data science users.

4) Finally, see the code and screenshots in "step_3_generate_curated_tables".  This includes customer_curated, which anonymizes the customerId and provides a reference for the serial number of the step sensor actually already in use by customers.  This also includes the table machine_learning_curated, which provides ML researchers the step sensor data appropriately joined to the accelerometer data, for customers who agreed to share their data with researchers, but while also keeping the customerId anonymous.
