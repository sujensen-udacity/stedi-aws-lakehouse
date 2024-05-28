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

# Script generated for node Read Accelerometer trusted
ReadAccelerometertrusted_node1716864028336 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="ReadAccelerometertrusted_node1716864028336")

# Script generated for node Read Customer trusted
ReadCustomertrusted_node1716863975895 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="ReadCustomertrusted_node1716863975895")

# Script generated for node Join customers with accel
SqlQuery2740 = '''
select distinct c.email, c.serialNumber from c join a on c.email = a.user
'''
Joincustomerswithaccel_node1716864073981 = sparkSqlQuery(glueContext, query = SqlQuery2740, mapping = {"c":ReadCustomertrusted_node1716863975895, "a":ReadAccelerometertrusted_node1716864028336}, transformation_ctx = "Joincustomerswithaccel_node1716864073981")

# Script generated for node Hash email address
detection_parameters = {"EMAIL": [{"action": "SHA256_HASH"}]}

entity_detector = EntityDetector()
Hashemailaddress_node1716864167369 = entity_detector.detect(Joincustomerswithaccel_node1716864073981, detection_parameters, "DetectedEntities", "HIGH")

# Script generated for node Select email and serialNum only
SqlQuery2741 = '''
select distinct email as customerId, serialNumber from myDataSource
'''
SelectemailandserialNumonly_node1716865237236 = sparkSqlQuery(glueContext, query = SqlQuery2741, mapping = {"myDataSource":Hashemailaddress_node1716864167369}, transformation_ctx = "SelectemailandserialNumonly_node1716865237236")

# Script generated for node Write Customer curated
WriteCustomercurated_node1716865152586 = glueContext.getSink(path="s3://my-stedi-bucket-for-project/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="WriteCustomercurated_node1716865152586")
WriteCustomercurated_node1716865152586.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
WriteCustomercurated_node1716865152586.setFormat("json")
WriteCustomercurated_node1716865152586.writeFrame(SelectemailandserialNumonly_node1716865237236)
job.commit()