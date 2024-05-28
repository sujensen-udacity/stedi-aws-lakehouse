import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node Read Accelerometer landing
ReadAccelerometerlanding_node1716862565778 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="ReadAccelerometerlanding_node1716862565778")

# Script generated for node Read Customer trusted
ReadCustomertrusted_node1716862635167 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="ReadCustomertrusted_node1716862635167")

# Script generated for node Join customer and accel
SqlQuery2811 = '''
select a.* from a join c on a.user = c.email
'''
Joincustomerandaccel_node1716862688808 = sparkSqlQuery(glueContext, query = SqlQuery2811, mapping = {"a":ReadAccelerometerlanding_node1716862565778, "c":ReadCustomertrusted_node1716862635167}, transformation_ctx = "Joincustomerandaccel_node1716862688808")

# Script generated for node Write Accelerometer trusted
WriteAccelerometertrusted_node1716862867944 = glueContext.getSink(path="s3://my-stedi-bucket-for-project/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="WriteAccelerometertrusted_node1716862867944")
WriteAccelerometertrusted_node1716862867944.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
WriteAccelerometertrusted_node1716862867944.setFormat("json")
WriteAccelerometertrusted_node1716862867944.writeFrame(Joincustomerandaccel_node1716862688808)
job.commit()