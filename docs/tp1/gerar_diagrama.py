"""Gera o diagrama de arquitetura BrasilMart em PNG."""

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import Glue, GlueDataCatalog, Redshift
from diagrams.aws.security import IAM
from diagrams.aws.storage import S3
from diagrams.onprem.analytics import Databricks, Spark
from diagrams.onprem.client import Users

with Diagram(
    "BrasilMart — Arquitetura Data Lakehouse",
    filename="docs/tp1/arquitetura_brasilmart",
    outformat="png",
    show=False,
    direction="LR",
    graph_attr={"fontsize": "14", "bgcolor": "white", "pad": "0.5"},
):
    # Fontes de dados
    with Cluster("Fontes de Dados (Olist Dataset)"):
        fontes = S3("CSVs Olist\n(orders, customers\nproducts, reviews...)")

    # Camada de ingestão
    with Cluster("Camada de Ingestão"):
        glue_job = Glue("AWS Glue Studio\nBatch ETL\n(CSV → Parquet)")
        databricks_ingest = Databricks("Databricks PySpark\nJSON Aninhado\n+ Streaming")

    # Storage S3
    with Cluster("Amazon S3 — Data Lakehouse"):
        raw = S3("Raw\n(CSVs originais)")
        bronze = S3("Bronze\n(Parquet / Delta)")
        silver = S3("Silver\n(Limpo / Padronizado)")
        gold = S3("Gold\n(Analítico / KPIs)")

    # Processamento
    with Cluster("Processamento — Databricks"):
        spark = Spark("Apache Spark\nPySpark Notebooks")

    # Governança
    with Cluster("Governança (LGPD)"):
        lake = GlueDataCatalog("Glue Data Catalog\n+ Lake Formation")
        iam = IAM("IAM + KMS\n(Segurança)")

    # Consumo
    with Cluster("Consumo Analítico"):
        redshift = Redshift("Amazon Redshift\nServerless")
        users = Users("BI / Analistas\nPower BI / SQL")

    # Fluxo principal
    fontes >> glue_job >> bronze
    fontes >> raw >> databricks_ingest >> bronze
    bronze >> spark >> silver >> gold
    gold >> redshift >> users

    # Governança transversal
    lake - Edge(style="dashed", color="gray") - bronze
    lake - Edge(style="dashed", color="gray") - silver
    lake - Edge(style="dashed", color="gray") - gold
    iam - Edge(style="dashed", color="gray") - raw
