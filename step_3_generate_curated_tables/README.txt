
The curated tables are ready for use by data scientists and the machine learning team.

(1) customer_curated shows the relationship between customer and serial number, only for customers who have actually been using the app (they have accelerometer data).  

To protect customer privacy, even those who have agreed to have their data included in the research, have had their personal information redacted (e.g. phone number, name, birthday) and only their hashed email address is visible (as customerId) in the curated tables.

(2) step_trainer_trusted could not be created until customer_curated table was.  This table includes all the step sensor readings, for customers who have agreed to share their data for research AND who have been using the app (have accelerometer data).  This resulted in 14460 sensor readings in the trusted table.  The step_trainer_trusted data is not particularly useful to data scientists until joined with the accelerometer data (in the next table, machine_learning_curated).

(3) Finally, we create the table machine_learning_curated by joining the trusted step sensor data with the trusted accelerometer data.  The final step sensor data in machine_learning_curated only includes the customers who have been using the mobile app (hence they have accelerometer data) and who have agree to share their data for research.  However, note that the machine_learning_curated table contains no PII, only a customerId. There are 43681 records in this table.

