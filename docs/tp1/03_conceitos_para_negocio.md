# TP1 - Parte 1.2: Conceitos e Ferramentas Explicados para Usuários de Negócio

> Este documento traduz os conceitos técnicos do projeto em linguagem acessível para stakeholders não especialistas em tecnologia — diretores, médicos, gestores e equipe de compliance.

---

## 1. Big Data — Por que Precisamos Pensar Grande

Imagine que cada atendimento na VidaPlus gera uma "ficha" digital: dados do paciente, exames solicitados, medicamentos prescritos, sinais vitais monitorados. Com 20 hospitais realizando milhões de atendimentos por ano, estamos falando de bilhões de registros que crescem a cada dia.

**Big Data** não é apenas "muitos dados" — é a combinação de:
- **Volume** enorme (terabytes de histórico clínico)
- **Variedade** de formatos (planilhas, documentos, dados de aparelhos médicos)
- **Velocidade** com que chegam (monitores de UTI enviando dados a cada segundo)

Ferramentas tradicionais como Excel ou bancos de dados simples não conseguem processar tudo isso em tempo hábil. Por isso, precisamos de tecnologias especializadas.

**Justificativa:** Com 2 milhões de pacientes e 12 sistemas diferentes, a VidaPlus gera dados em volume e complexidade que só podem ser tratados com tecnologias de Big Data.

---

## 2. Data Lakehouse — O Melhor dos Dois Mundos

Historicamente, empresas tinham duas opções para armazenar dados:
- **Data Lake**: um grande "reservatório" que aceita qualquer tipo de dado (como um grande arquivo morto), mas sem organização — difícil encontrar o que se precisa.
- **Data Warehouse**: um "armário organizado" com dados limpos e prontos para relatórios, mas rígido e caro para armazenar tudo.

O **Data Lakehouse** combina os dois: é um reservatório que aceita qualquer dado (como o Lake) mas com organização, qualidade e segurança (como o Warehouse).

**Analogia hospitalar:** é como ter um prontuário eletrônico universal que aceita qualquer tipo de documento (laudos, imagens, receitas em papel digitalizado) mas que organiza tudo automaticamente para que o médico encontre o que precisa em segundos.

**Justificativa:** A VidaPlus precisa armazenar dados brutos de 12 sistemas diferentes (flexibilidade do Lake) e ao mesmo tempo entregar relatórios confiáveis para diretoria e médicos (organização do Warehouse).

---

## 3. Arquitetura em Camadas (Bronze, Silver, Gold) — Do Bruto ao Refinado

Assim como minérios passam por etapas de refino, os dados da VidaPlus passam por camadas:

| Camada | Analogia | O que contém |
|--------|----------|-------------|
| **Raw/Bronze** | Minério bruto recém-extraído | Dados exatamente como chegam dos sistemas — sem modificação |
| **Silver** | Metal purificado | Dados limpos, padronizados e com qualidade validada |
| **Gold** | Joia lapidada | Dados prontos para uso — relatórios, dashboards, modelos |

**Por que não ir direto ao Gold?** Porque precisamos preservar o dado original para auditoria (LGPD exige isso) e porque cada público precisa de um nível diferente de refino.

**Justificativa:** Essa organização garante rastreabilidade (de onde veio cada dado), qualidade progressiva e atende requisitos regulatórios.

---

## 4. Apache Spark — O Motor de Processamento

O **Apache Spark** é como uma fábrica com centenas de trabalhadores processando dados simultaneamente. Enquanto um computador normal processa dados um por vez (como uma pessoa lendo prontuários um a um), o Spark distribui o trabalho entre dezenas ou centenas de máquinas em paralelo.

**Exemplo prático:** para calcular a taxa de readmissão cruzando dados de 20 hospitais e 5 anos de histórico (centenas de milhões de registros), um computador comum levaria horas ou dias. O Spark faz isso em minutos.

**Justificativa:** O volume de dados da VidaPlus (bilhões de registros históricos + dados em tempo real de UTI) exige processamento distribuído que só o Spark oferece com custo-benefício adequado.

---

## 5. Amazon S3 — O Armazém Seguro

O **Amazon S3** (Simple Storage Service) é como um cofre digital na nuvem com capacidade ilimitada. É onde todos os dados da VidaPlus serão armazenados — desde os dados brutos até as tabelas analíticas prontas.

Características importantes:
- **Segurança**: criptografia automática (como trancar cada gaveta do cofre individualmente)
- **Durabilidade**: 99,999999999% (11 noves) — praticamente impossível perder um arquivo
- **Custo otimizado**: dados antigos são movidos automaticamente para "prateleiras mais baratas"

**Justificativa:** Dados de saúde exigem armazenamento com altíssima segurança e durabilidade. O S3 é o padrão do mercado para isso, com custo controlado por políticas automáticas.

---

## 6. AWS Glue — O Catalogador e Integrador

O **AWS Glue** funciona como um bibliotecário digital: ele automaticamente cataloga todos os dados armazenados no S3, registrando o que existe, onde está e qual o formato. Além disso, ele executa tarefas de movimentação e transformação de dados.

- **Glue Data Catalog**: o "catálogo da biblioteca" — um índice de todos os dados disponíveis
- **Glue Studio**: ferramenta visual para criar fluxos de processamento de dados sem programação complexa

**Justificativa:** Com dados vindos de 12 sistemas diferentes, precisamos de um catálogo centralizado para saber exatamente o que temos e onde está — essencial para governança e LGPD.

---

## 7. AWS Lake Formation — O Guardião dos Dados

O **Lake Formation** é o "segurança" da plataforma. Ele controla quem pode ver o quê:
- Um médico pode ver os dados clínicos dos seus pacientes, mas não dados financeiros
- Um analista de BI pode ver dados agregados, mas não dados individuais de pacientes
- A equipe de faturamento vê dados financeiros, mas com CPF e nome anonimizados

Esse controle acontece no nível de coluna e linha da tabela — é como poder dizer "fulano pode ver a tabela de pacientes, mas apenas as colunas de idade e cidade, nunca o nome ou CPF".

**Justificativa:** A LGPD classifica dados de saúde como "sensíveis" (Art. 11), exigindo controles rigorosos de acesso. O Lake Formation oferece exatamente esse controle granular, com logs de auditoria completos.

---

## 8. Amazon Redshift — O Analista Veloz

O **Amazon Redshift** é um banco de dados otimizado para consultas analíticas rápidas. Enquanto bancos de dados tradicionais são bons para operações do dia a dia (inserir um novo paciente, atualizar um agendamento), o Redshift é especializado em responder perguntas complexas sobre grandes volumes de dados.

**Exemplo:** "Qual a taxa de readmissão por hospital, especialidade e faixa etária nos últimos 3 anos?" — uma pergunta que envolveria cruzar centenas de milhões de registros. O Redshift responde isso em segundos.

**Justificativa:** A diretoria e analistas precisam de respostas rápidas a perguntas complexas sobre toda a rede hospitalar. O Redshift é o componente que entrega essa velocidade sobre o dado refinado.

---

## 9. Databricks — A Plataforma de Ciência de Dados

O **Databricks** é uma plataforma completa para engenheiros e cientistas de dados trabalharem com Spark. Pense nele como um "laboratório digital" onde a equipe de dados pode:
- Conectar-se a todos os dados da VidaPlus
- Escrever e executar análises usando Python (uma linguagem de programação popular)
- Criar modelos de inteligência artificial (ex: prever quais pacientes têm risco de readmissão)
- Colaborar em notebooks interativos (como documentos vivos que misturam código, gráficos e texto)

**Justificativa:** O Databricks oferece uma experiência integrada de desenvolvimento que acelera o trabalho da equipe de dados e se conecta nativamente com o S3 e o Lake Formation.

---

## 10. Apache Airflow — O Maestro da Orquestra

O **Apache Airflow** é como um maestro que coordena toda a orquestra de dados. Ele garante que cada tarefa aconteça na ordem certa e no momento certo:
1. Primeiro, extrair dados do sistema de prontuário
2. Depois, limpar e padronizar
3. Em seguida, carregar nas tabelas analíticas
4. Por fim, notificar a equipe de BI que os dados estão atualizados

Se alguma etapa falha, o Airflow avisa a equipe de TI automaticamente.

**Justificativa:** Com dados vindos de múltiplas fontes em diferentes horários, precisamos de um orquestrador que garanta que tudo funcione em sequência e que falhas sejam detectadas e comunicadas imediatamente.

---

## 11. Formatos de Dados: Parquet e Delta Lake

### Parquet
O **Parquet** é um formato de arquivo otimizado para análise. Se um CSV é como uma tabela no Excel (simples, mas lento para grandes volumes), o Parquet é como essa mesma tabela comprimida e organizada de forma que o computador consiga ler apenas as colunas necessárias — muito mais rápido.

### Delta Lake
O **Delta Lake** adiciona ao Parquet capacidades de "desfazer alterações" (como o Ctrl+Z do Word) e garante que mesmo com múltiplas pessoas escrevendo dados ao mesmo tempo, nada se perca ou corrompa.

**Justificativa:** Parquet reduz o custo de armazenamento em até 75% comparado com CSV e acelera consultas em até 100x. Delta Lake garante confiabilidade, essencial para dados de saúde onde erros podem ter consequências graves.

---

## Resumo Visual

```
┌─────────────────────────────────────────────────────────────┐
│                    FLUXO DE DADOS VIDAPLUS                   │
│                                                              │
│  Sistemas      Ingestão       Refino         Consumo        │
│  Hospitalares  & Armazenamento                               │
│                                                              │
│  ┌─────┐      ┌──────┐     ┌──────┐      ┌──────────┐      │
│  │ HIS │─┐    │      │     │      │      │Dashboards│      │
│  └─────┘ │    │Bronze│────▶│Silver│─────▶│Relatórios│      │
│  ┌─────┐ ├───▶│(Bruto)│     │(Limpo)│      │ Modelos  │      │
│  │ LIS │─┤    │      │     │      │      │   IA     │      │
│  └─────┘ │    └──────┘     └──────┘      └──────────┘      │
│  ┌─────┐ │        │             │              │             │
│  │ IoT │─┘    ┌───┴─────────────┴──────────────┴──┐         │
│  └─────┘      │  Governança (Lake Formation/LGPD) │         │
│               └───────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────┘
```
