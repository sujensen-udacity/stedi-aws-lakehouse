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

# Script generated for node Read Customer curated
ReadCustomercurated_node1716866367841 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="ReadCustomercurated_node1716866367841")

# Script generated for node Read Step trainer landing
ReadSteptrainerlanding_node1716866419110 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="ReadSteptrainerlanding_node1716866419110")

# Script generated for node Join on serial number
SqlQuery2805 = '''
select s.* from s join c on s.serialnumber = c.serialnumber
'''
Joinonserialnumber_node1716866604036 = sparkSqlQuery(glueContext, query = SqlQuery2805, mapping = {"s":ReadSteptrainerlanding_node1716866419110, "c":ReadCustomercurated_node1716866367841}, transformation_ctx = "Joinonserialnumber_node1716866604036")

# Script generated for node Write Step trainer trusted
WriteSteptrainertrusted_node1716866684781 = glueContext.getSink(path="s3://my-stedi-bucket-for-project/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="WriteSteptrainertrusted_node1716866684781")
WriteSteptrainertrusted_node1716866684781.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
WriteSteptrainertrusted_node1716866684781.setFormat("json")
WriteSteptrainertrusted_node1716866684781.writeFrame(Joinonserialnumber_node1716866604036)
job.commit()