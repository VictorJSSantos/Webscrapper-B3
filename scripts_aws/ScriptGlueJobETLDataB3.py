import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Dados Brutos B3
DadosBrutosB3_node1737014629146 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://s3-fiap-etl-250461282134/raw/"], "recurse": True}, transformation_ctx="DadosBrutosB3_node1737014629146")

# Script generated for node Aggregate
Aggregate_node1737015113494 = sparkAggregate(glueContext, parentFrame = DadosBrutosB3_node1737014629146, groups = ["tipo", "info_extraction_date", "acao"], aggs = [["qtde_teorica", "sum"]], transformation_ctx = "Aggregate_node1737015113494")

# Script generated for node Renomear campo data
Renomearcampodata_node1737015358586 = RenameField.apply(frame=Aggregate_node1737015113494, old_name="info_extraction_date", new_name="data_extracao", transformation_ctx="Renomearcampodata_node1737015358586")

# Script generated for node Renomear campo qtde
Renomearcampoqtde_node1737015442511 = RenameField.apply(frame=Renomearcampodata_node1737015358586, old_name="`sum(qtde_teorica)`", new_name="quantidade", transformation_ctx="Renomearcampoqtde_node1737015442511")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    acao,
    tipo,
	quantidade,
	data_extracao,
    current_date() AS data_atual,
    datediff(current_date(), data_extracao) AS dias_diferenca
FROM tblRawB3;
'''
SQLQuery_node1737015520881 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"tblRawB3":Renomearcampoqtde_node1737015442511}, transformation_ctx = "SQLQuery_node1737015520881")

# Script generated for node Dados B3 Refinados
EvaluateDataQuality().process_rows(frame=SQLQuery_node1737015520881, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737014580168", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
DadosB3Refinados_node1737015649079 = glueContext.getSink(path="s3://s3-fiap-etl-250461282134/refined/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["data_extracao", "acao"], enableUpdateCatalog=True, transformation_ctx="DadosB3Refinados_node1737015649079")
DadosB3Refinados_node1737015649079.setCatalogInfo(catalogDatabase="fiap",catalogTableName="tbl_dados_b3_refinados_glue")
DadosB3Refinados_node1737015649079.setFormat("glueparquet", compression="snappy")
DadosB3Refinados_node1737015649079.writeFrame(SQLQuery_node1737015520881)
job.commit()