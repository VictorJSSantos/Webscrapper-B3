import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1734380945638 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://bucket-teste-parquet-fiap-mlet/27-11-24.parquet"], "recurse": True}, transformation_ctx="AmazonS3_node1734380945638")

# Script generated for node Aggregate
Aggregate_node1734382244907 = sparkAggregate(glueContext, parentFrame = AmazonS3_node1734380945638, groups = ["tipo", "info_extraction_date", "participacao_percentual", "acao"], aggs = [["qtde_teorica", "sum"], ["info_extraction_date", "max"], ["info_extraction_date", "min"]], transformation_ctx = "Aggregate_node1734382244907")

# Script generated for node Rename Field
RenameField_node1734382654480 = RenameField.apply(frame=Aggregate_node1734382244907, old_name="info_extraction_date", new_name="DataRef", transformation_ctx="RenameField_node1734382654480")

# Script generated for node Rename Field
RenameField_node1734382689821 = RenameField.apply(frame=RenameField_node1734382654480, old_name="`sum(qtde_teorica)`", new_name="QtdeAcao", transformation_ctx="RenameField_node1734382689821")

# Script generated for node Rename Field
RenameField_node1734382748414 = RenameField.apply(frame=RenameField_node1734382689821, old_name="`min(info_extraction_date)`", new_name="MenorData", transformation_ctx="RenameField_node1734382748414")

# Script generated for node Rename Field
RenameField_node1734382784353 = RenameField.apply(frame=RenameField_node1734382748414, old_name="`max(info_extraction_date)`", new_name="MaiorData", transformation_ctx="RenameField_node1734382784353")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RenameField_node1734382784353, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734380821395", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1734382827877 = glueContext.getSink(path="s3://bucket-teste-parquet-fiap-mlet/refined/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["DataRef", "acao"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1734382827877")
AmazonS3_node1734382827877.setCatalogInfo(catalogDatabase="fiap",catalogTableName="bovespa_etl")
AmazonS3_node1734382827877.setFormat("glueparquet", compression="snappy")
AmazonS3_node1734382827877.writeFrame(RenameField_node1734382784353)
job.commit()