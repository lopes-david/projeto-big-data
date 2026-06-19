# TP1 - Parte 1.1.1: Entrevista com Stakeholders

## Contexto

As questões abaixo foram elaboradas para serem aplicadas aos principais stakeholders da BrasilMart. As respostas obtidas servem como requisitos norteadores para o desenvolvimento da Plataforma de Dados Unificada.

---

## Roteiro de Entrevista

### Questão 1 — CEO / Diretoria Executiva
**Pergunta:** Qual o principal gap estratégico que nos impede de crescer? Temos dados suficientes sobre nossos clientes para competir com grandes players como Mercado Livre e Amazon?

**Resposta esperada:** "O nosso maior gap é a visão do cliente. Sabemos quantos pedidos fizemos, mas não sabemos quem são nossos melhores clientes, por que eles voltam ou por que não voltam. Sem isso, não conseguimos personalizar a experiência nem focar investimento em retenção versus aquisição."

**Requisito derivado:** A plataforma deve gerar segmentação RFM (Recência, Frequência, Monetário) consolidada na camada Gold, identificando clusters de clientes por valor e comportamento.

---

### Questão 2 — Diretor de Marketing
**Pergunta:** Como vocês definem e medem o sucesso de uma campanha de retenção hoje? Quais dados vocês usam e quais dados vocês precisariam ter?

**Resposta esperada:** "Medimos abertura de e-mail e clique, mas não conseguimos correlacionar com compras subsequentes porque o dado está em sistemas separados. Precisaríamos do histórico completo de compra por cliente, data da última compra e valor médio do ticket para segmentar campanhas."

**Requisito derivado:** Tabela Gold `dim_clientes_rfm` com segmentação: Recência (dias desde última compra), Frequência (total de pedidos), Monetário (gasto total), classificação de segmento (Champions, At Risk, Lost, etc.).

---

### Questão 3 — Diretor de Operações / Logística
**Pergunta:** Qual o maior problema operacional hoje? Conseguimos prever quando um pedido vai atrasar antes que o cliente reclame?

**Resposta esperada:** "Não. Sabemos do atraso quando o cliente abre um ticket de reclamação. Temos SLA de 5 dias para a maioria dos pedidos, mas não temos um modelo que nos diga quais pedidos têm risco de atraso logo após a aprovação. Isso nos custou R$ 2,3M em reembolsos no ano passado."

**Requisito derivado:** Feature engineering na camada Silver com cálculo de `delta_entrega` (diferença entre data estimada e data real), análise de lead time por região/vendedor/categoria. Base para modelo preditivo de atraso (TPs futuros).

---

### Questão 4 — Diretor de Marketplace / Sellers
**Pergunta:** Como monitoramos a qualidade dos vendedores? Existe um score consolidado de performance que considere vendas, avaliações, prazo e cancelamentos?

**Resposta esperada:** "Não temos um score unificado. Avaliamos volume de vendas separadamente, reclamações separadamente, cancelamentos separadamente. Um vendedor com muitas vendas mas péssimas avaliações não é detectado automaticamente — precisamos cruzar manualmente."

**Requisito derivado:** Tabela Gold `dim_sellers_score` com seller_score composto: nota média das reviews (peso 40%), taxa de entrega no prazo (peso 30%), taxa de cancelamento (peso 20%), volume de pedidos (peso 10%).

---

### Questão 5 — Equipe de BI
**Pergunta:** Quais relatórios você precisa que levam mais de 1 hora para extrair hoje? O que seria diferente se os dados estivessem em tempo (quasi) real?

**Resposta esperada:** "O relatório de GMV por categoria e estado leva 3 horas porque cruzamos 4 planilhas manualmente. Se fosse automatizado e atualizado diariamente, poderíamos reagir a quedas de venda em tempo real — hoje só percebemos na reunião semanal."

**Requisito derivado:** Pipeline diário automatizado (Airflow/Glue) para atualizar tabela Gold `fato_vendas_diarias` com GMV, ticket médio, volume por categoria e estado.

---

### Questão 6 — Equipe de Fraude / Financeiro
**Pergunta:** Qual o perfil típico de transação fraudulenta que vocês já identificaram? Quais campos de dados seriam mais úteis para um modelo de detecção?

**Resposta esperada:** "Vemos padrões como: pagamento com cartão de débito de valor alto (acima de R$ 500) em conta nova (menos de 30 dias), entrega para CEP diferente do CEP de cadastro, e múltiplas compras em categorias de alto valor no mesmo dia. Precisaríamos ter esses dados históricos consolidados para treinar um modelo."

**Requisito derivado:** Feature store na camada Silver com: `customer_age_days`, `payment_zip_vs_customer_zip_match`, `daily_purchase_count`, `avg_payment_value_30d`. Preparação para modelo de detecção de fraude.

---

### Questão 7 — Cientista de Dados
**Pergunta:** Qual seria o primeiro modelo de ML que você desenvolveria se tivesse acesso a dados históricos limpos e integrados?

**Resposta esperada:** "Predição de churn — identificar clientes que fizeram 1 ou 2 compras e têm alta probabilidade de não voltar, para acionar uma campanha de retenção proativa. Precisaria do histórico de compras, reviews, categoria, e se houve algum problema logístico na última compra."

**Requisito derivado:** Tabela Silver `silver_customer_features` com todas as features necessárias para treinamento: histórico de compras, reviews recebidos, categorias preferidas, experiência logística, tempo de resposta do vendedor.

---

### Questão 8 — Diretor de Marketplace — Catálogo
**Pergunta:** Quantos produtos do catálogo atual você estima que estão "mortos" (sem vendas, sem cliques)? O que fazer com eles?

**Resposta esperada:** "Estimamos que 20% dos produtos não vendem há mais de 90 dias, mas não sabemos exatamente porque não cruzamos o catálogo com os dados de venda por produto. Se soubéssemos, poderíamos fazer um programa de incentivo para os vendedores atualizarem ou removerem esses produtos."

**Requisito derivado:** Tabela Gold `dim_produtos_performance` com `days_since_last_sale`, `total_revenue_90d`, `avg_review_score`, `return_rate`. Sinalização de produtos para curadoria.

---

### Questão 9 — Diretor de Marketing — Regionalização
**Pergunta:** A nossa base de clientes está distribuída uniformemente pelo Brasil? Existe potencial não explorado em regiões específicas?

**Resposta esperada:** "Sabemos que SP e RJ concentram a maioria das vendas, mas não temos análise de penetração de mercado: quantos clientes em potencial existem em cada estado versus quantos já compramos? Isso determinaria onde focar as próximas campanhas de aquisição."

**Requisito derivado:** Tabela Gold com análise geoespacial: clientes e GMV por estado/cidade (cruzando `olist_customers` com `olist_geolocation`), identificação de estados com alta densidade geográfica e baixa penetração de clientes.

---

### Questão 10 — CEO — Expansão e M&A
**Pergunta:** Se adquirirmos outro marketplace regional nos próximos 6 meses, qual o maior desafio para integrar os dados deles à nossa plataforma?

**Resposta esperada:** "O maior desafio é a heterogeneidade: cada plataforma usa IDs diferentes para clientes, categorias de produto codificadas de forma diferente, e às vezes nem têm dados de CEP padronizados. Hoje uma integração dessas leva 6 meses de trabalho manual."

**Requisito derivado:** A arquitetura Bronze deve aceitar schemas variáveis (schema-on-read) e a Silver deve implementar um processo de canonicalização — IDs unificados (customer_unique_id), categorias padronizadas (via tabela de tradução), CEPs normalizados (5 dígitos). Onboarding de nova fonte < 4 semanas.

---

### Questão 11 — Equipe de Reviews / Customer Success
**Pergunta:** Qual o padrão de comportamento de um cliente que deixa uma avaliação negativa (score 1 ou 2)? O problema é geralmente do produto ou da entrega?

**Resposta esperada:** "Analisamos manualmente uma amostra e percebemos que 65% das avaliações negativas mencionam atraso ou produto diferente do anunciado. Mas não temos como confirmar isso em escala — precisaríamos de análise de texto das reviews cruzada com dados de entrega."

**Requisito derivado:** Pipeline NLP básico na camada Gold para classificação de reviews (atraso, produto diferente, defeito, etc.) cruzada com `delta_entrega`. Base para análise de causa raiz de insatisfação (TPs futuros).

---

### Questão 12 — Diretor de Operações — Sazonalidade
**Pergunta:** A plataforma está preparada para absorver picos de demanda como Black Friday? Temos visibilidade de quantas horas antes do pico precisamos escalar recursos?

**Resposta esperada:** "Não. Nos últimos dois anos, a infraestrutura ficou instável durante a Black Friday porque escalamos manualmente com base em intuição. Precisaríamos de dados históricos de volume por hora para prever o pico e acionar o auto-scaling com antecedência."

**Requisito derivado:** Série temporal horária de eventos de pedido na camada Silver, com detecção de padrões sazonais (dia da semana, feriados, datas comemorativas). Base para modelo de forecasting de demanda.

---

## Resumo dos Requisitos Derivados

| # | Requisito | Prioridade | Fonte |
|---|-----------|-----------|-------|
| R1 | Segmentação RFM de clientes na camada Gold | Crítica | Q1, Q2 |
| R2 | Cálculo de delta_entrega e análise de SLA por vendedor/região | Alta | Q3 |
| R3 | Seller Score composto (reviews + entrega + cancelamento) | Alta | Q4 |
| R4 | Pipeline diário de GMV por categoria e estado | Alta | Q5 |
| R5 | Feature store para modelo de fraude | Média | Q6 |
| R6 | Feature store para modelo de churn | Alta | Q7 |
| R7 | Catálogo de produtos com performance analytics | Média | Q8 |
| R8 | Análise geoespacial de clientes por CEP/estado | Média | Q9 |
| R9 | Schema flexível na Bronze + canonicalização na Silver | Alta | Q10 |
| R10 | Análise de causa raiz de reviews negativos (NLP básico) | Baixa | Q11 |
| R11 | Série temporal horária para forecasting de demanda | Média | Q12 |
