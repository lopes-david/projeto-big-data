# TP3 — Roteiro de Prints e Evidências

> **Como usar:** Siga cada seção na ordem. Copie o código para o Databricks, execute e tire print nos pontos marcados com 📸.
> Os textos explicativos podem ser copiados diretamente para o relatório/apresentação.

---

# 1. DATABRICKS — Pipeline Medallion com DLT (Camada Silver)

---

## 1.1 — Pipeline DLT que lê da Camada Bronze (TP2)

**O que provar:** Que existe um pipeline Delta Live Tables em Python que lê todas as 9 tabelas Bronze do TP2 e produz tabelas Silver.

---

### Texto explicativo (para o relatório):

O Delta Live Tables (DLT) é o framework declarativo do Databricks para construção de pipelines de dados. Em vez de escrever código imperativo (leia, transforme, grave), declaramos as tabelas e suas transformações usando decorators Python (`@dlt.table`). O DLT cuida automaticamente da execução, ordenação de dependências, gerenciamento de estado e recuperação de falhas.

O pipeline `pb-brasilmart-silver` lê as 9 tabelas Managed da Camada Bronze (`pb_brasilmart.bronze.*`), que foram criadas no TP2 como Delta Tables no Unity Catalog. Cada tabela Silver é definida com `@dlt.table` e lê da Bronze via `spark.table("pb_brasilmart.bronze.<tabela>")`.

A vantagem do DLT sobre notebooks PySpark convencionais:
- **Declarativo**: defino O QUE quero, não COMO executar
- **Grafo automático**: o DLT resolve dependências entre tabelas sozinho
- **Qualidade integrada**: Expectations validam dados em tempo de execução
- **Idempotente**: posso re-executar sem efeitos colaterais

---

### Passo 1 — Criar o pipeline DLT no Databricks

Vá em **Workflows → Delta Live Tables → Create Pipeline** e configure:

| Campo | Valor |
|-------|-------|
| Pipeline name | `pb-brasilmart-silver` |
| Product edition | `Advanced` |
| Source code | Notebook `PB-TP3-01_dlt_silver` |
| Destination → Catalog | `pb_brasilmart` |
| Destination → Target schema | `silver` |
| Cluster mode | `Enhanced` |
| Cluster policy | Default |

📸 **PRINT 1:** Tela de configuração do pipeline DLT preenchida (mostrando nome, source, catalog, schema)

---

### Passo 2 — Código do notebook DLT (mostrar no relatório)

Abra o notebook `PB-TP3-01_dlt_silver` no Databricks e mostre o código:

```python
import dlt
from pyspark.sql import functions as F

# Exemplo: tabela Silver Orders lendo da Bronze
@dlt.table(
    name="orders",
    comment="Pedidos limpos com tempos decorridos (segundos) entre eventos e status de entrega",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
@dlt.expect("Entrega_Atraso_Alerta",
    "delta_entrega_dias <= 30 OR delta_entrega_dias IS NULL"
)
def silver_orders():
    df = spark.table("pb_brasilmart.bronze.orders")
    # ... transformações ...
```

📸 **PRINT 2:** Notebook aberto no Databricks mostrando o `import dlt` e pelo menos 2 `@dlt.table` (orders e customers)

---

### Passo 3 — Executar o pipeline

Clique em **Start** no pipeline. Aguarde a conclusão.

📸 **PRINT 3:** Grafo visual do pipeline DLT em execução ou concluído (tela do DLT mostrando as tabelas conectadas com setas)

📸 **PRINT 4:** Status final do pipeline mostrando "Completed" com todas as tabelas em verde

---

### Passo 4 — Verificar tabelas criadas no Unity Catalog

```sql
USE CATALOG pb_brasilmart;
SHOW TABLES IN silver;
```

📸 **PRINT 5:** Resultado do `SHOW TABLES IN silver` mostrando as 11 tabelas criadas (orders, customers, items, payments, reviews, products, sellers, geolocation, category_translation, orders_enriched, items_enriched)

---

### Passo 5 — Verificar contagens (prova de que os dados foram processados)

Execute no notebook ou SQL Editor:

```python
silver_tables = [
    "orders", "customers", "items", "payments", "reviews",
    "products", "sellers", "geolocation", "category_translation",
    "orders_enriched", "items_enriched"
]

print(f"{'Tabela Silver':<30} {'Registros':>12}")
print("-" * 45)
for t in silver_tables:
    count = spark.table(f"silver.{t}").count()
    print(f"{t:<30} {count:>12,}")
```

📸 **PRINT 6:** Saída com contagens de todas as 11 tabelas Silver (prova que os dados da Bronze foram lidos e processados)

---

### Passo 6 — Prova da leitura da Bronze

Execute para mostrar que a Silver foi derivada da Bronze:

```sql
-- Comparar contagens Bronze vs Silver
SELECT 'bronze.orders' AS tabela, COUNT(*) AS registros FROM pb_brasilmart.bronze.orders
UNION ALL
SELECT 'silver.orders', COUNT(*) FROM pb_brasilmart.silver.orders
UNION ALL
SELECT 'bronze.customers', COUNT(*) FROM pb_brasilmart.bronze.customers
UNION ALL
SELECT 'silver.customers', COUNT(*) FROM pb_brasilmart.silver.customers
UNION ALL
SELECT 'bronze.items', COUNT(*) FROM pb_brasilmart.bronze.items
UNION ALL
SELECT 'silver.items', COUNT(*) FROM pb_brasilmart.silver.items;
```

📸 **PRINT 7:** Tabela comparativa Bronze vs Silver mostrando que Silver foi derivada da Bronze (contagens iguais ou Silver ≤ Bronze quando há DROP)

---

### Resumo de prints — Item 1.1

| # | O que capturar | Prova |
|---|----------------|-------|
| 1 | Config do pipeline DLT | Pipeline DLT existe e aponta para Bronze/Silver |
| 2 | Código do notebook | Usa `import dlt`, `@dlt.table`, lê de `pb_brasilmart.bronze.*` |
| 3 | Grafo visual DLT | Mostra as dependências entre tabelas |
| 4 | Pipeline concluído | Todas as tabelas processadas com sucesso |
| 5 | SHOW TABLES IN silver | 11 tabelas criadas no Unity Catalog |
| 6 | Contagens Silver | Dados foram processados (registros > 0) |
| 7 | Bronze vs Silver | Silver foi derivada da Bronze |
