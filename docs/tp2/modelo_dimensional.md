# 2.2 — Modelo Dimensional (Kimball) — BrasilMart

## Visão Geral

Modelo Star Schema com **1 tabela Fato** e **6 Dimensões**, grão: **1 linha por item de pedido**.

```
                    ┌──────────────┐
                    │  dim_tempo   │
                    │  (data)      │
                    └──────┬───────┘
                           │
┌──────────────┐   ┌───────┴────────┐   ┌──────────────────┐
│ dim_cliente  ├───┤                ├───┤  dim_produto      │
│ (RFM)        │   │  fato_vendas   │   │  (categoria)      │
└──────────────┘   │                │   └──────────────────┘
                   │  price         │
┌──────────────┐   │  freight       │   ┌──────────────────┐
│dim_localizacao├──┤  review_score  ├───┤  dim_vendedor     │
│ (geo)        │   │  delta_entrega │   │  (score)          │
└──────────────┘   └───────┬────────┘   └──────────────────┘
                           │
                    ┌──────┴───────┐
                    │dim_pagamento │
                    │ (método)     │
                    └──────────────┘
```

## Tabela Fato

### fato_vendas (grão: 1 item de pedido)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| order_id | VARCHAR | FK pedido |
| order_item_id | INT | Sequencial do item |
| date_key | INT | FK dim_tempo |
| customer_key | VARCHAR | FK dim_cliente |
| product_key | VARCHAR | FK dim_produto |
| seller_key | VARCHAR | FK dim_vendedor |
| location_key | VARCHAR | FK dim_localizacao |
| payment_type | VARCHAR | FK dim_pagamento |
| price | DECIMAL | Preço do item |
| freight_value | DECIMAL | Valor do frete |
| total_item_value | DECIMAL | price + freight |
| review_score | INT | Nota da avaliação (1-5) |
| delta_entrega_dias | INT | Dias de atraso (positivo) ou antecipação (negativo) |
| status_entrega | VARCHAR | no_prazo / atrasado / pendente |

**Justificativa do grão:** item de pedido é o nível mais granular que permite analisar tanto volume de vendas quanto performance por produto, vendedor e região.

## Dimensões

### 1. dim_tempo (obrigatória)

| Coluna | Exemplo |
|--------|---------|
| date_key | 20170115 |
| data | 2017-01-15 |
| ano | 2017 |
| trimestre | 1 |
| mes | 1 |
| nome_mes | Janeiro |
| dia | 15 |
| dia_semana | Domingo |
| dia_semana_num | 1 |
| is_fim_semana | true |
| semana_ano | 3 |

**Justificativa:** Permite análises temporais em qualquer granularidade (dia, mês, trimestre, ano), identificação de sazonalidade (Black Friday, Natal), e comparações YoY.

### 2. dim_localizacao (obrigatória)

| Coluna | Exemplo |
|--------|---------|
| location_key | 01001 |
| cep_prefixo | 01001 |
| cidade | são paulo |
| estado | SP |
| regiao | Sudeste |

**Justificativa:** O Olist opera em todos os 26 estados. Análise regional é essencial para campanhas de marketing geolocalizadas, otimização logística e identificação de mercados sub-penetrados (requisito do Diretor de Marketing).

### 3. dim_cliente

| Coluna | Exemplo |
|--------|---------|
| customer_key | abc123 |
| customer_unique_id | xyz789 |
| cidade | são paulo |
| estado | SP |
| total_pedidos | 3 |
| total_gasto | 450.00 |
| recency_days | 45 |
| rfm_segment | Champions |

**Justificativa:** Core do projeto — visão 360° do cliente. A segmentação RFM (Recência, Frequência, Monetário) permite campanhas personalizadas e identificação de churn (requisito CEO e Diretor de Marketing).

### 4. dim_produto

| Coluna | Exemplo |
|--------|---------|
| product_key | prod456 |
| categoria | electronics |
| peso_kg | 1.25 |
| comprimento_cm | 30.0 |
| largura_cm | 20.0 |
| altura_cm | 10.0 |
| volume_cm3 | 6000 |
| qtd_fotos | 4 |

**Justificativa:** Permite análise de performance por categoria, identificação de produtos "mortos" (sem venda 90+ dias), e correlação entre atributos do produto (peso, fotos) e taxa de review negativo (requisito Diretor de Marketplace).

### 5. dim_vendedor

| Coluna | Exemplo |
|--------|---------|
| seller_key | sel789 |
| cidade | curitiba |
| estado | PR |
| total_pedidos | 150 |
| avg_review_score | 4.2 |
| on_time_rate | 92.5 |
| cancel_rate | 3.1 |
| seller_score | 78.5 |
| seller_tier | Bom |

**Justificativa:** 12% dos vendedores geram 60% das avaliações negativas. O score composto (reviews 40%, on-time 30%, cancel 20%, volume 10%) permite gestão proativa do marketplace (requisito Diretor de Marketplace).

### 6. dim_pagamento

| Coluna | Exemplo |
|--------|---------|
| payment_type | credit_card |
| payment_group | cartao |
| descricao | Cartão de Crédito |

**Justificativa:** Análise de mix de pagamento por região e perfil de cliente. Essencial para detecção de fraude (cartão de débito + valor alto + conta nova) e negociação de taxas com operadoras (requisito Equipe de Fraude).

## Justificativa da Estrutura

O modelo Star Schema foi escolhido porque:

1. **Simplicidade** — analistas de BI fazem queries diretas sem joins complexos
2. **Performance** — Redshift otimiza star schemas nativamente com DistKey/SortKey
3. **Flexibilidade** — cada dimensão pode evoluir independentemente (SCD Type 2 nos TPs futuros)
4. **Alinhamento com requisitos** — cada dimensão atende diretamente a um stakeholder entrevistado no TP1
