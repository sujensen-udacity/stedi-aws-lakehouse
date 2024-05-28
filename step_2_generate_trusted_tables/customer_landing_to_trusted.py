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

# Script generated for node Read Customer Landing
ReadCustomerLanding_node1716853270996 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="ReadCustomerLanding_node1716853270996")

# Script generated for node Filter out no share
SqlQuery2824 = '''
select * from myDataSource where sharewithresearchasofdate is not null
'''
Filteroutnoshare_node1716853283679 = sparkSqlQuery(glueContext, query = SqlQuery2824, mapping = {"myDataSource":ReadCustomerLanding_node1716853270996}, transformation_ctx = "Filteroutnoshare_node1716853283679")

# Script generated for node Write Customer Trusted
WriteCustomerTrusted_node1716854126770 = glueContext.getSink(path="s3://my-stedi-bucket-for-project/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="WriteCustomerTrusted_node1716854126770")
WriteCustomerTrusted_node1716854126770.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
WriteCustomerTrusted_node1716854126770.setFormat("json")
WriteCustomerTrusted_node1716854126770.writeFrame(Filteroutnoshare_node1716853283679)
job.commit()