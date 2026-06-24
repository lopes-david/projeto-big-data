#!/bin/bash
# TP4 1.3 — Configura monitoramento: SNS topic + CloudWatch Alarm para falha do Step Functions
# Uso: bash infra/aws/setup_monitoring.sh

set -euo pipefail

REGION="sa-east-1"
ACCOUNT_ID="234828142988"
ALERT_EMAIL="david.lopes@al.infnet.edu.br"
STATE_MACHINE_NAME="pb-brasilmart-orchestration"
SNS_TOPIC_NAME="pb-brasilmart-alertas"

echo "=== TP4 1.3 — Monitoramento e Alertas ==="

# 1. Criar topico SNS para alertas
echo "[1/4] Criando topico SNS: $SNS_TOPIC_NAME"
SNS_TOPIC_ARN=$(aws sns create-topic \
  --name "$SNS_TOPIC_NAME" \
  --region "$REGION" \
  --tags Key=Project,Value=pb-brasilmart Key=TP,Value=tp4 \
  --query 'TopicArn' \
  --output text)
echo "  Topic ARN: $SNS_TOPIC_ARN"

# 2. Inscrever e-mail no topico
echo "[2/4] Inscrevendo e-mail: $ALERT_EMAIL"
aws sns subscribe \
  --topic-arn "$SNS_TOPIC_ARN" \
  --protocol email \
  --notification-endpoint "$ALERT_EMAIL" \
  --region "$REGION"
echo "  IMPORTANTE: Confirme a inscricao no e-mail recebido!"

# 3. Criar alarme CloudWatch para falha do Step Functions
echo "[3/4] Criando alarme CloudWatch para falha do Step Functions"
aws cloudwatch put-metric-alarm \
  --alarm-name "pb-brasilmart-stepfunctions-falha" \
  --alarm-description "Alerta quando o Job do Step Functions pb-brasilmart-orchestration falha" \
  --namespace "AWS/States" \
  --metric-name "ExecutionsFailed" \
  --dimensions Name=StateMachineArn,Value="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}" \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --ok-actions "$SNS_TOPIC_ARN" \
  --tags Key=Project,Value=pb-brasilmart Key=TP,Value=tp4 \
  --region "$REGION"
echo "  Alarme criado: pb-brasilmart-stepfunctions-falha"

# 4. Criar alarme para timeout (execucoes que excedem tempo esperado)
echo "[4/4] Criando alarme CloudWatch para timeout do Step Functions"
aws cloudwatch put-metric-alarm \
  --alarm-name "pb-brasilmart-stepfunctions-timeout" \
  --alarm-description "Alerta quando o Step Functions excede 30 minutos de execucao" \
  --namespace "AWS/States" \
  --metric-name "ExecutionTime" \
  --dimensions Name=StateMachineArn,Value="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE_NAME}" \
  --statistic Maximum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1800000 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --treat-missing-data notBreaching \
  --alarm-actions "$SNS_TOPIC_ARN" \
  --tags Key=Project,Value=pb-brasilmart Key=TP,Value=tp4 \
  --region "$REGION"
echo "  Alarme criado: pb-brasilmart-stepfunctions-timeout"

echo ""
echo "=== Monitoramento configurado com sucesso ==="
echo "Recursos criados:"
echo "  - SNS Topic: $SNS_TOPIC_ARN"
echo "  - CloudWatch Alarm: pb-brasilmart-stepfunctions-falha (ExecutionsFailed >= 1)"
echo "  - CloudWatch Alarm: pb-brasilmart-stepfunctions-timeout (ExecutionTime > 30min)"
echo ""
echo "Proximos passos:"
echo "  1. Confirme a inscricao SNS no e-mail $ALERT_EMAIL"
echo "  2. Teste o alarme: aws cloudwatch set-alarm-state --alarm-name pb-brasilmart-stepfunctions-falha --state-value ALARM --state-reason 'Teste manual'"
