# TP1 - Parte 1.1.1: Entrevista com Stakeholders

## Contexto

As questões abaixo foram elaboradas para serem aplicadas aos principais stakeholders da VidaPlus Saúde. As respostas obtidas servirão como requisitos norteadores para o desenvolvimento da Plataforma de Dados Unificada.

---

## Roteiro de Entrevista

### Questão 1 — Diretoria Executiva (CEO/CFO)
**Pergunta:** Quais são os 3 principais indicadores de performance (KPIs) que a diretoria acompanha hoje, e quais deles vocês gostariam de acompanhar mas não conseguem por falta de dados integrados?

**Resposta esperada:** "Acompanhamos faturamento, taxa de ocupação e ticket médio. Gostaríamos de acompanhar custo por paciente ao longo da jornada completa, taxa de readmissão por unidade e lifetime value do paciente, mas hoje esses dados estão fragmentados em sistemas diferentes."

**Requisito derivado:** A plataforma deve consolidar dados financeiros e clínicos para gerar KPIs transversais como custo por jornada do paciente e taxa de readmissão por unidade.

---

### Questão 2 — Diretor Médico (CMO)
**Pergunta:** Quanto tempo em média um médico gasta buscando informações de histórico do paciente antes de uma consulta? Que tipo de informação é mais difícil de obter?

**Resposta esperada:** "Um médico gasta em média 15 minutos por consulta buscando histórico. Os dados mais difíceis de obter são resultados de exames feitos em outras unidades, histórico de medicamentos prescritos em emergências e laudos de imagem."

**Requisito derivado:** A camada Gold deve conter uma visão consolidada do paciente com histórico completo de exames, prescrições e laudos, acessível em tempo real.

---

### Questão 3 — Diretor de TI (CIO)
**Pergunta:** Quais sistemas legados existem hoje na rede, e quais são os formatos de dados e protocolos de integração disponíveis em cada um?

**Resposta esperada:** "Temos 5 sistemas de prontuário diferentes (3 exportam via HL7/FHIR, 2 só exportam CSV), o LIS exporta JSON com estruturas aninhadas por painel de exames, e o sistema de agendamento tem uma API REST. Alguns hospitais adquiridos ainda usam planilhas Excel."

**Requisito derivado:** A camada de ingestão deve suportar múltiplos formatos (JSON aninhado, CSV, HL7/FHIR) e múltiplos protocolos (API REST, exportação de arquivos em lote), com tratamento de schemas heterogêneos.

---

### Questão 4 — Coordenadora de Enfermagem
**Pergunta:** Em situações de emergência, quais informações do paciente são mais críticas e qual o tempo máximo aceitável para obtê-las?

**Resposta esperada:** "Alergias, medicamentos em uso, condições crônicas e último hemograma. Precisamos disso em no máximo 30 segundos. Hoje, se o paciente não lembra ou não traz documentos, ligamos para outras unidades e pode levar horas."

**Requisito derivado:** Dados críticos do paciente (alergias, medicamentos, condições crônicas) devem estar na camada Gold com latência máxima de ingestão de 5 minutos e disponíveis via consulta rápida.

---

### Questão 5 — Equipe de BI / Analistas de Dados
**Pergunta:** Quais análises vocês executam hoje e quais ferramentas utilizam? Quais análises vocês gostariam de fazer mas não conseguem com a infraestrutura atual?

**Resposta esperada:** "Fazemos relatórios mensais de ocupação e faturamento usando Excel e Power BI conectados diretamente aos bancos transacionais. Gostaríamos de fazer análise preditiva de readmissão, segmentação de pacientes por perfil de risco, e análise de eficiência de protocolos clínicos — mas não temos os dados unificados nem poder computacional para isso."

**Requisito derivado:** A plataforma deve disponibilizar tabelas analíticas otimizadas (camada Gold) em formato compatível com Power BI/Tableau, além de suportar processamento Spark para modelos de machine learning.

---

### Questão 6 — DPO / Equipe de Compliance
**Pergunta:** Como é feito o controle de acesso aos dados dos pacientes hoje? Existe rastreabilidade de quem acessou qual informação?

**Resposta esperada:** "Cada sistema tem seu próprio controle de acesso, sem padronização. Não temos log unificado de acessos. Em caso de auditoria LGPD, precisamos consultar cada sistema individualmente, o que leva semanas."

**Requisito derivado:** A plataforma deve implementar controle de acesso granular (column-level e row-level security) via AWS Lake Formation, com logs de auditoria centralizados e capacidade de atender requisições de titulares (direito de acesso, portabilidade, exclusão).

---

### Questão 7 — Gestor de Operações Hospitalares
**Pergunta:** Como é feita a previsão de demanda por leitos, salas cirúrgicas e estoque de medicamentos? Com que antecedência vocês conseguem planejar?

**Resposta esperada:** "Usamos médias históricas manuais com base em planilhas. Conseguimos planejar no máximo uma semana à frente. Em períodos de surto, somos pegos de surpresa e precisamos realocar recursos de última hora."

**Requisito derivado:** A plataforma deve suportar modelos preditivos de demanda com horizonte de pelo menos 30 dias, alimentados por dados históricos de internações, sazonalidade e dados epidemiológicos externos.

---

### Questão 8 — Operadora de Plano de Saúde (Parceiro)
**Pergunta:** Quais são os maiores problemas no processo de faturamento e autorização entre a VidaPlus e as operadoras? Qual o percentual de glosas?

**Resposta esperada:** "O maior problema é a inconsistência nos códigos TISS/TUSS enviados. O percentual de glosas está em torno de 12%, sendo que a média do mercado é 7%. Isso acontece porque cada unidade codifica os procedimentos de forma diferente."

**Requisito derivado:** A camada Silver deve padronizar códigos de procedimentos (TISS/TUSS) e a camada Gold deve gerar relatórios de faturamento com validação prévia para reduzir glosas.

---

### Questão 9 — Diretor Médico (CMO) — Pesquisa e Qualidade
**Pergunta:** A VidaPlus participa de estudos clínicos ou programas de vigilância epidemiológica? Como os dados são extraídos e reportados para órgãos reguladores (ANVISA, Ministério da Saúde)?

**Resposta esperada:** "Sim, participamos de programas de vigilância de infecção hospitalar e notificações compulsórias. Hoje, cada hospital envia relatórios manualmente, com formatos diferentes. Os dados não são cruzados entre unidades para identificar tendências."

**Requisito derivado:** A plataforma deve suportar agregações epidemiológicas cross-hospital na camada Gold, com exportação automatizada em formatos exigidos pelos órgãos reguladores.

---

### Questão 10 — Diretoria Executiva (CEO/CFO) — Expansão
**Pergunta:** A VidaPlus planeja adquirir novos hospitais nos próximos anos? Qual é o maior desafio de TI durante o processo de integração de uma nova unidade?

**Resposta esperada:** "Sim, planejamos adquirir 3 hospitais nos próximos 2 anos. O maior desafio é integrar os sistemas de TI: leva de 12 a 18 meses para migrar dados e unificar processos. Durante esse período, os dados do hospital adquirido ficam isolados."

**Requisito derivado:** A arquitetura deve ser extensível para onboarding rápido de novas fontes de dados, com schemas flexíveis na camada Bronze e pipelines parametrizáveis para novas unidades.

---

### Questão 11 — Pacientes (via pesquisa de satisfação)
**Pergunta:** Qual foi sua experiência ao ser atendido em mais de uma unidade da VidaPlus? Você precisou repetir exames ou re-informar seu histórico médico?

**Resposta esperada:** "Sim, quando fui transferido para outra unidade tive que refazer exames de sangue que tinha feito há 2 dias. O médico não tinha acesso ao meu prontuário da outra unidade."

**Requisito derivado:** A visão 360° do paciente na camada Gold deve ser a fonte única de verdade, eliminando redundância de exames e garantindo continuidade do cuidado entre unidades.

---

### Questão 12 — Diretor de TI (CIO) — Segurança
**Pergunta:** Qual é a estratégia atual de backup, disaster recovery e criptografia dos dados de pacientes? Já houve incidentes de segurança?

**Resposta esperada:** "Cada unidade tem sua própria política de backup. A criptografia é aplicada em alguns sistemas, mas não em todos. Não houve incidentes graves, mas em auditoria identificamos que dados de pacientes estavam em buckets S3 sem criptografia adequada."

**Requisito derivado:** Todos os buckets S3 devem ter criptografia SSE-S3/SSE-KMS habilitada, versionamento ativo, e políticas de ciclo de vida. A governança via Lake Formation deve garantir acesso mínimo necessário (principle of least privilege).

---

## Resumo dos Requisitos Derivados

| # | Requisito | Prioridade | Fonte |
|---|-----------|-----------|-------|
| R1 | KPIs transversais (custo por jornada, readmissão por unidade) | Alta | Q1 |
| R2 | Visão consolidada do paciente na camada Gold | Crítica | Q2, Q11 |
| R3 | Ingestão multi-formato (JSON, CSV, HL7/FHIR, APIs) | Alta | Q3 |
| R4 | Latência máxima de 5 min para dados críticos | Crítica | Q4 |
| R5 | Tabelas Gold compatíveis com BI + suporte a ML | Alta | Q5 |
| R6 | Controle de acesso granular + logs de auditoria (LGPD) | Crítica | Q6, Q12 |
| R7 | Modelos preditivos de demanda (horizonte 30 dias) | Média | Q7 |
| R8 | Padronização TISS/TUSS e validação de faturamento | Alta | Q8 |
| R9 | Agregações epidemiológicas cross-hospital | Média | Q9 |
| R10 | Arquitetura extensível para onboarding de novas unidades | Alta | Q10 |
| R11 | Criptografia, versionamento e lifecycle em todos os buckets | Crítica | Q12 |
