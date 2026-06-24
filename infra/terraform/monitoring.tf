# TP4 1.3 — Monitoramento: SNS + CloudWatch Alarms para Step Functions

resource "aws_sns_topic" "brasilmart_alertas" {
  name = "pb-brasilmart-alertas"

  tags = {
    TP      = "tp4"
    Project = "pb-brasilmart"
  }
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.brasilmart_alertas.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Alarme: Step Functions — execucao falhou
resource "aws_cloudwatch_metric_alarm" "stepfunctions_falha" {
  alarm_name          = "pb-brasilmart-stepfunctions-falha"
  alarm_description   = "Alerta quando o Job do Step Functions pb-brasilmart-orchestration falha"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = "arn:aws:states:${var.aws_region}:${var.account_id}:stateMachine:pb-brasilmart-orchestration"
  }

  alarm_actions = [aws_sns_topic.brasilmart_alertas.arn]
  ok_actions    = [aws_sns_topic.brasilmart_alertas.arn]

  tags = { TP = "tp4" }
}

# Alarme: Step Functions — execucao excedeu 30 minutos
resource "aws_cloudwatch_metric_alarm" "stepfunctions_timeout" {
  alarm_name          = "pb-brasilmart-stepfunctions-timeout"
  alarm_description   = "Alerta quando o Step Functions excede 30 minutos de execucao"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionTime"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Maximum"
  threshold           = 1800000
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = "arn:aws:states:${var.aws_region}:${var.account_id}:stateMachine:pb-brasilmart-orchestration"
  }

  alarm_actions = [aws_sns_topic.brasilmart_alertas.arn]

  tags = { TP = "tp4" }
}
