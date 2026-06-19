# 1.4 — Defesa Técnica: Data Lakehouse e Apache Spark

**Por que Data Lakehouse?**
O projeto tem 9 fontes de dados em formatos diferentes. O Data Lakehouse combina a flexibilidade de aceitar qualquer formato (como um Data Lake) com a organização e qualidade dos dados em camadas (como um Data Warehouse). O Delta Lake garante que os dados nunca fiquem corrompidos e permite voltar a versões anteriores para auditoria.

**Por que Spark e não só Glue?**
O Glue é usado para ingestão batch dos CSVs simples — é suficiente e barato para isso. O Spark entra onde o Glue não performa:

- **JSON aninhado:** o dataset de pedidos tem 4 níveis de aninhamento. O Glue não lida bem com isso. O PySpark resolve nativamente.
- **1 milhão de registros geográficos:** joins desse volume causam problemas de memória no Glue. O Spark usa broadcast join e otimização automática.
- **Streaming:** o Glue tem latência mínima de 60s. O Spark Structured Streaming processa em segundos.
