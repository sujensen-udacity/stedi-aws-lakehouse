import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Read Step trainer trusted
ReadSteptrainertrusted_node1716867459529 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="ReadSteptrainertrusted_node1716867459529")

# Script generated for node Read Accel trusted
ReadAcceltrusted_node1716867501526 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="ReadAcceltrusted_node1716867501526")

# Script generated for node Hash email address
detection_parameters = {"EMAIL": [{"action": "SHA256_HASH"}]}

entity_detector = EntityDetector()
Hashemailaddress_node1716867573585 = entity_detector.detect(ReadAcceltrusted_node1716867501526, detection_parameters, "DetectedEntities", "HIGH")

# Script generated for node Select email as customerid
SqlQuery2660 = '''
select user as customerid, timestamp, x, y, z from myDataSource
'''
Selectemailascustomerid_node1716867662768 = sparkSqlQuery(glueContext, query = SqlQuery2660, mapping = {"myDataSource":Hashemailaddress_node1716867573585}, transformation_ctx = "Selectemailascustomerid_node1716867662768")

# Script generated for node Join for ML curated table
SqlQuery2661 = '''
select s.*, a.customerid, a.x, a.y, a.z from s left join a on s.sensorreadingtime = a.timestamp
'''
JoinforMLcuratedtable_node1716867784165 = sparkSqlQuery(glueContext, query = SqlQuery2661, mapping = {"a":Selectemailascustomerid_node1716867662768, "s":ReadSteptrainertrusted_node1716867459529}, transformation_ctx = "JoinforMLcuratedtable_node1716867784165")

# Script generated for node Write ML curated table
WriteMLcuratedtable_node1716868076117 = glueContext.getSink(path="s3://my-stedi-bucket-for-project/ml_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="WriteMLcuratedtable_node1716868076117")
WriteMLcuratedtable_node1716868076117.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
WriteMLcuratedtable_node1716868076117.setFormat("json")
WriteMLcuratedtable_node1716868076117.writeFrame(JoinforMLcuratedtable_node1716867784165)
job.commit()