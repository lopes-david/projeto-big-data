# 1.2 — Conceitos e Ferramentas (para usuários de negócio)

**Big Data**
O projeto envolve mais de 1,5 milhão de registros entre pedidos, clientes, pagamentos e avaliações. Ferramentas tradicionais como Excel não conseguem processar esse volume. Big Data é o conjunto de tecnologias que resolve isso.

**Data Lakehouse**
É onde todos os dados ficam armazenados na nuvem, organizados em camadas de qualidade crescente (Raw → Bronze → Silver → Gold), do dado bruto até o dado pronto para análise.

**Amazon S3**
Armazenamento em nuvem onde os dados do projeto ficam guardados. Seguro, barato e com capacidade ilimitada.

**AWS Glue**
Ferramenta que lê os arquivos CSV e os converte automaticamente para um formato mais eficiente (Parquet), catalogando tudo para facilitar consultas.

**AWS Lake Formation**
Controla quem pode acessar quais dados. Um analista de marketing vê dados de clientes, mas não vê dados de pagamento — o Lake Formation garante isso.

**Apache Spark**
Motor de processamento distribuído. Enquanto o Glue processa arquivos simples, o Spark é usado quando os dados são complexos (JSON aninhado, 1 milhão de registros geográficos) ou exigem mais poder de processamento.

**Databricks**
Plataforma onde o Spark roda. É onde os notebooks PySpark são desenvolvidos e executados.

**Amazon Redshift**
Banco de dados otimizado para consultas analíticas rápidas. É onde analistas e ferramentas de BI consultam os dados prontos da camada Gold.

**Apache Airflow**
Orquestrador que garante que os pipelines de dados rodem na ordem certa e no horário certo, com alertas em caso de falha.
