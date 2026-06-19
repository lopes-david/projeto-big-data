# TP1 - Parte 1.1: Tema do Projeto e Problema de Negócio

## Tema: Plataforma de Dados Unificada para Marketplace de E-commerce — Visão 360° do Cliente

---

## Resumo do Negócio

A **BrasilMart** é um marketplace de e-commerce de grande porte que conecta vendedores independentes a milhões de consumidores em todo o Brasil, operando em todos os 26 estados e no Distrito Federal. Fundada em 2014, a empresa cresceu rapidamente via aquisições de plataformas regionais e hoje hospeda mais de **3.000 vendedores ativos**, processa em média **100.000 pedidos por mês** e conta com uma base de **99.000+ clientes únicos**.

A plataforma opera como intermediadora: os vendedores publicam seus produtos, os consumidores compram e a BrasilMart gerencia a logística, os pagamentos e o relacionamento pós-venda — incluindo avaliações e suporte.

**Base de dados real utilizada:** Dataset público do marketplace Olist, disponível no Kaggle, contendo dados reais de 2016 a 2018 com as seguintes dimensões:

| Dataset | Registros | Descrição |
|---------|-----------|-----------|
| `olist_orders_dataset.csv` | 99.441 | Pedidos com status e timestamps do ciclo de vida |
| `olist_customers_dataset.csv` | 99.441 | Cadastro de clientes (único por pedido) |
| `olist_order_items_dataset.csv` | 112.650 | Itens de cada pedido (produto, vendedor, preço) |
| `olist_order_payments_dataset.csv` | 103.886 | Pagamentos (tipo, parcelas, valor) |
| `olist_order_reviews_dataset.csv` | 104.719 | Avaliações dos clientes |
| `olist_products_dataset.csv` | 32.951 | Catálogo de produtos |
| `olist_sellers_dataset.csv` | 3.095 | Cadastro de vendedores |
| `olist_geolocation_dataset.csv` | 1.000.163 | Dados geográficos (CEP → lat/lng) |
| `product_category_name_translation.csv` | 70 | Traduções de categorias (PT → EN) |
| **Total** | **~1,56 milhão** | |

---

## Problema de Negócio

Apesar do crescimento, a BrasilMart enfrenta problemas críticos causados pela ausência de uma plataforma de dados integrada:

| Problema | Impacto |
|----------|---------|
| **Sem visão 360° do cliente** — histórico de compras, comportamento, valor do cliente (LTV) não consolidados | Campanhas de marketing genéricas, taxa de recompra 23% abaixo do benchmark do setor |
| **Análise de performance de vendedores reativa** — gestores sabem de problemas de qualidade só quando acumulam reclamações | 12% dos vendedores responsáveis por 60% das avaliações negativas (score ≤ 2) |
| **Logística sem analytics preditivo** — atrasos nas entregas só detectados após ocorrência | 8% dos pedidos entregues com atraso > 5 dias; custo de reembolso R$ 2,3M/ano |
| **Churn de clientes não monitorado** — sem segmentação por comportamento, impossível identificar clientes em risco | 70% dos clientes fazem apenas 1 compra; lifetime value médio de R$ 150 |
| **Fraudes em pagamentos não detectadas em tempo real** — análise manual pós-fato | Perda estimada de R$ 800K/ano com chargebacks |
| **Catálogo de produtos com qualidade variável** — sem análise de dados de reviews e retorno para curadoria | 18% dos produtos ativos sem venda nos últimos 90 dias |

---

## Stakeholders

| Stakeholder | Papel | Interesse no Projeto |
|-------------|-------|---------------------|
| **CEO / Diretoria** | Decisores estratégicos | Crescimento de GMV, redução de churn, market share |
| **Diretor de Marketing** | Growth e retenção | Segmentação de clientes, LTV, campanhas personalizadas |
| **Diretor de Operações** | Logística e fulfillment | Predição de atrasos, otimização de rotas, SLA |
| **Diretor de Marketplace** | Gestão de vendedores | Performance de sellers, qualidade de catálogo |
| **Equipe de BI / Analistas** | Consumidores analíticos | Dashboards, relatórios, análises ad-hoc |
| **Equipe de Dados (Eng./Cientistas)** | Produtores e consumidores | Pipelines, modelos de ML, qualidade de dados |
| **Equipe de Fraude** | Prevenção de perdas | Detecção de anomalias em pagamentos |
| **Vendedores (Sellers)** | Parceiros comerciais | Relatórios de performance, visibilidade de estoque |

---

## Usuários Finais

1. **Analistas de Marketing** — consumirão a segmentação RFM (Recência, Frequência, Monetário) de clientes na camada Gold para campanhas personalizadas.
2. **Gestores de Operações** — acompanharão KPIs de entrega (on-time rate, tempo médio) por região e vendedor.
3. **Equipe de Curadoria de Catálogo** — usarão dashboards de produtos com baixa performance, alta taxa de devolução ou reviews negativos.
4. **Cientistas de Dados** — consumirão as tabelas Silver para treinar modelos de predição de churn, recomendação e detecção de fraude.
5. **Vendedores (via portal self-service)** — acessarão relatórios próprios de performance: vendas, avaliações e taxa de entrega no prazo.

---

## Justificativa da Escolha do Tema

O e-commerce é um dos setores que mais justifica tecnologias de Big Data pelos 5 Vs:

1. **Volume**: 1,56 milhão de registros em apenas 2 anos de operação de uma plataforma de médio porte. Escalonando para um marketplace nacional, seriam bilhões de eventos por ano.

2. **Variedade**: dados estruturados (pedidos, pagamentos, clientes), semi-estruturados (reviews com texto livre e metadata) e geoespaciais (1 milhão de registros de geolocalização de CEPs).

3. **Velocidade**: a cada novo pedido realizado, múltiplos eventos são gerados em cascata — aprovação de pagamento, notificação ao vendedor, geração de etiqueta logística, atualização de estoque — exigindo processamento near-real-time.

4. **Veracidade**: dados de múltiplas fontes (sistema de pedidos, gateway de pagamento, transportadoras, sistema de reviews) com necessidade de reconciliação e deduplicação.

5. **Valor**: insights sobre comportamento de compra, predição de churn, otimização de rotas e detecção de fraude têm impacto financeiro direto e mensurável.

Adicionalmente, o dataset da Olist é **real e público**, o que permite demonstrar a plataforma com dados que refletem padrões reais de e-commerce brasileiro, incluindo sazonalidade, distribuição geográfica concentrada no Sudeste e comportamento de pagamento (preferência por cartão de crédito parcelado).
