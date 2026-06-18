# TP1 - Parte 1.1: Tema do Projeto e Problema de Negócio

## Tema: Plataforma de Dados Unificada para Rede Hospitalar — Visão 360° do Paciente

---

## Resumo do Negócio

A **VidaPlus Saúde** é uma rede hospitalar de grande porte que opera em 8 estados brasileiros, composta por 20 hospitais, 60 clínicas ambulatoriais e mais de 2 milhões de pacientes ativos. A rede emprega aproximadamente 15.000 profissionais de saúde e realiza cerca de 8 milhões de atendimentos por ano.

Ao longo de duas décadas de expansão — incluindo aquisições de hospitais regionais — a VidaPlus acumulou uma infraestrutura de TI fragmentada: cada unidade utiliza sistemas diferentes para prontuário eletrônico (HIS), laboratório (LIS), radiologia (RIS) e agendamento. Os dados dos pacientes estão espalhados em pelo menos 12 sistemas distintos, sem integração entre eles.

---

## Problema de Negócio

A ausência de uma visão unificada do paciente gera impactos críticos:

| Problema | Impacto Estimado |
|----------|-----------------|
| **Exames duplicados** — sem acesso ao histórico completo, médicos solicitam exames que o paciente já realizou em outra unidade | R$ 50 milhões/ano em desperdício |
| **Tempo de consolidação** — reunir informações de um paciente atendido em múltiplas unidades leva em média 72 horas | Atraso em diagnósticos e decisões clínicas |
| **Readmissões evitáveis** — sem análise preditiva, pacientes de alto risco recebem alta sem acompanhamento adequado | Taxa de readmissão 18% acima da média do setor |
| **Conformidade LGPD** — dados sensíveis de saúde (Art. 11) armazenados sem governança centralizada | Risco regulatório e multas de até 2% do faturamento |
| **Ineficiência operacional** — alocação de leitos, escalas médicas e gestão de estoque de medicamentos baseada em intuição, não em dados | Ocupação de leitos 15% abaixo do ótimo |

---

## Stakeholders

| Stakeholder | Papel | Interesse no Projeto |
|-------------|-------|---------------------|
| **Diretoria Executiva (CEO/CFO)** | Decisores estratégicos | ROI, redução de custos operacionais, compliance |
| **Diretor Médico (CMO)** | Liderança clínica | Qualidade do atendimento, redução de erros médicos, protocolos baseados em dados |
| **Diretor de TI (CIO)** | Liderança tecnológica | Arquitetura escalável, integração de sistemas, segurança |
| **Coordenadores de Enfermagem** | Gestão de cuidados | Acesso rápido ao histórico do paciente, alertas clínicos |
| **Equipe de BI / Analistas de Dados** | Consumidores analíticos | Dashboards, relatórios, modelos preditivos |
| **Equipe de Compliance / DPO** | Governança e privacidade | LGPD, consentimento, anonimização, auditoria |
| **Gestores de Operações** | Eficiência hospitalar | Ocupação de leitos, gestão de filas, estoque de insumos |
| **Operadoras de Planos de Saúde** | Parceiros financeiros | Padronização de dados para faturamento (TISS/TUSS), redução de glosas |

---

## Usuários Finais

1. **Médicos e enfermeiros** — acessarão dashboards clínicos com visão consolidada do paciente durante o atendimento.
2. **Analistas de dados** — utilizarão tabelas analíticas (camada Gold) para criar relatórios de performance hospitalar, epidemiologia e predição de readmissões.
3. **Gestores operacionais** — consumirão indicadores de ocupação, tempo de espera e eficiência por unidade.
4. **Equipe de compliance** — acessarão relatórios de governança, linhagem de dados e logs de acesso.

---

## Justificativa da Escolha do Tema

A saúde é um dos setores que mais gera dados no mundo — desde registros eletrônicos de pacientes, resultados de exames laboratoriais, imagens médicas, até dados de dispositivos IoT de monitoramento. Esta complexidade e volume justificam plenamente o uso de tecnologias de Big Data:

1. **Volume**: milhões de registros de consultas, exames e prescrições gerados diariamente em uma rede de 20 hospitais.
2. **Variedade**: dados estruturados (tabelas de agendamento), semi-estruturados (JSON de prontuários eletrônicos, HL7/FHIR) e não-estruturados (laudos em texto livre, PDFs de exames).
3. **Velocidade**: dados de monitores de sinais vitais em UTI que chegam em tempo real (streaming), exigindo processamento near-real-time para alertas clínicos.
4. **Veracidade**: dados de múltiplas fontes heterogêneas que precisam ser reconciliados e validados para garantir confiabilidade clínica.
5. **Valor**: insights como predição de readmissão, detecção precoce de surtos epidemiológicos e otimização de recursos hospitalares têm impacto direto na vida dos pacientes e na sustentabilidade financeira da rede.

Adicionalmente, o setor de saúde possui exigências rigorosas de **governança e privacidade** (LGPD, HIPAA), o que torna a implementação de um Data Lakehouse com controles de acesso granulares um requisito técnico real, não apenas acadêmico.
