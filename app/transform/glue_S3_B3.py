import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1732509179765 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://fiap-tc-modulo-2-raw"], "recurse": True},
    transformation_ctx="AmazonS3_node1732509179765",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1732509270616 = DynamicFrame.fromDF(
    AmazonS3_node1732509179765.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1732509270616",
)

# Script generated for node Mudar nome de variáveis
Mudarnomedevariveis_node1732509277079 = ApplyMapping.apply(
    frame=DropDuplicates_node1732509270616,
    mappings=[
        ("codigo", "string", "code", "string"),
        ("acao", "string", "company", "string"),
        ("tipo", "string", "type", "string"),
        ("qtde_teorica", "bigint", "quantity", "long"),
        ("participacao_percentual", "double", "share_percentage", "double"),
        ("info_extraction_date", "string", "info_extraction_date", "string"),
    ],
    transformation_ctx="Mudarnomedevariveis_node1732509277079",
)

# Script generated for node Aggregate
Aggregate_node1732509369190 = sparkAggregate(
    glueContext,
    parentFrame=Mudarnomedevariveis_node1732509277079,
    groups=["info_extraction_date", "type", "code"],
    aggs=[["share_percentage", "sum"], ["quantity", "sum"]],
    transformation_ctx="Aggregate_node1732509369190",
)

# Script generated for node Mudar nome das agregações
Mudarnomedasagregaes_node1732594251546 = ApplyMapping.apply(
    frame=Aggregate_node1732509369190,
    mappings=[
        ("info_extraction_date", "string", "info_extraction_date", "string"),
        ("type", "string", "type", "string"),
        ("code", "string", "code", "string"),
        ("`sum(share_percentage)`", "double", "sum_share_percentage", "double"),
        ("`sum(quantity)`", "long", "sum_quantity", "long"),
    ],
    transformation_ctx="Mudarnomedasagregaes_node1732594251546",
)

# Script generated for node Amazon S3
AmazonS3_node1732509417226 = glueContext.getSink(
    path="s3://fiap-tc-modulo-2-processed",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["info_extraction_date", "code"],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1732509417226",
)
AmazonS3_node1732509417226.setCatalogInfo(
    catalogDatabase="fiap-tc-modulo-2",
    catalogTableName="fiap-tc-modulo-2-glue-catalog-table",
)
AmazonS3_node1732509417226.setFormat("glueparquet", compression="snappy")
AmazonS3_node1732509417226.writeFrame(Mudarnomedasagregaes_node1732594251546)
job.commit()
