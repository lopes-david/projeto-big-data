"""Gera diagrama de arquitetura BrasilMart usando matplotlib."""

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

fig, ax = plt.subplots(1, 1, figsize=(22, 14))
ax.set_xlim(0, 22)
ax.set_ylim(0, 14)
ax.axis("off")
fig.patch.set_facecolor("#F8F9FA")
ax.set_facecolor("#F8F9FA")

# ─── HELPERS ────────────────────────────────────────────────────────────────

def box(ax, x, y, w, h, label, sublabel="", color="#FFFFFF", border="#333333",
        fontsize=9, subsize=7.5, bold=False):
    rect = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.08",
                          facecolor=color, edgecolor=border, linewidth=1.5, zorder=3)
    ax.add_patch(rect)
    weight = "bold" if bold else "normal"
    cy = y + h / 2 + (0.12 if sublabel else 0)
    ax.text(x + w/2, cy, label, ha="center", va="center",
            fontsize=fontsize, fontweight=weight, color="#1A1A2E", zorder=4)
    if sublabel:
        ax.text(x + w/2, y + h/2 - 0.22, sublabel, ha="center", va="center",
                fontsize=subsize, color="#555555", zorder=4)

def cluster(ax, x, y, w, h, title, color="#E8F4FD", border="#2196F3"):
    rect = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.1",
                          facecolor=color, edgecolor=border, linewidth=2,
                          linestyle="--", zorder=1)
    ax.add_patch(rect)
    ax.text(x + 0.15, y + h - 0.01, title, ha="left", va="top",
            fontsize=8.5, fontweight="bold", color=border, zorder=2)

def arrow(ax, x1, y1, x2, y2, color="#333333", style="->", lw=1.8):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle=style, color=color,
                                lw=lw, connectionstyle="arc3,rad=0.0"),
                zorder=5)

def arrow_dash(ax, x1, y1, x2, y2, color="#888888"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-", color=color, lw=1.2,
                                linestyle="dashed", connectionstyle="arc3,rad=0.0"),
                zorder=5)

# ─── TÍTULO ─────────────────────────────────────────────────────────────────
ax.text(11, 13.5, "BrasilMart — Arquitetura Data Lakehouse (AWS + Databricks)",
        ha="center", va="center", fontsize=15, fontweight="bold", color="#1A1A2E")
ax.text(11, 13.1, "Visão 360° do Cliente | Dataset: Olist Brazilian E-Commerce",
        ha="center", va="center", fontsize=10, color="#555555")

# ─── CLUSTER: FONTES ────────────────────────────────────────────────────────
cluster(ax, 0.3, 9.2, 3.8, 3.5, "① Fontes de Dados (Olist)", "#FFF8E1", "#FF9800")
box(ax, 0.6, 11.5, 3.2, 0.9, "olist_orders.csv",       "99.441 pedidos",      "#FFF3E0", "#FF9800")
box(ax, 0.6, 10.5, 3.2, 0.9, "olist_customers.csv",    "99.441 clientes",     "#FFF3E0", "#FF9800")
box(ax, 0.6, 9.5,  3.2, 0.9, "olist_geolocation.csv",  "1.000.163 registros", "#FFF3E0", "#FF9800")

# ─── CLUSTER: INGESTÃO ──────────────────────────────────────────────────────
cluster(ax, 4.5, 9.2, 4.2, 3.5, "② Ingestão", "#E8F5E9", "#4CAF50")
box(ax, 4.7, 11.2, 3.8, 1.2, "AWS Glue Studio",
    "Batch ETL\nCSV → Parquet\nJob Bookmark (incr.)", "#E8F5E9", "#4CAF50", bold=True)
box(ax, 4.7, 9.5,  3.8, 1.5, "Databricks PySpark",
    "JSON Aninhado (4 níveis)\n+ Streaming Simulado\n(Auto Loader S3)", "#E3F2FD", "#1565C0", bold=True)

# ─── CLUSTER: S3 LAKEHOUSE ──────────────────────────────────────────────────
cluster(ax, 9.1, 5.8, 5.2, 7.2, "③ Amazon S3 — Data Lakehouse (4 Camadas)", "#F3E5F5", "#7B1FA2")
box(ax, 9.4, 11.5, 4.6, 1.2, "RAW",    "CSVs originais Olist\nImutável | 7 anos", "#F3E5F5", "#9C27B0", fontsize=10, bold=True)
box(ax, 9.4, 9.8,  4.6, 1.5, "BRONZE", "Parquet + Delta Lake\nSem transformação | 3 anos", "#E8EAF6", "#3949AB", fontsize=10, bold=True)
box(ax, 9.4, 8.0,  4.6, 1.5, "SILVER", "Dados limpos e padronizados\nDelta | 2 anos", "#E0F7FA", "#00838F", fontsize=10, bold=True)
box(ax, 9.4, 6.1,  4.6, 1.6, "GOLD",   "Tabelas Analíticas\ndim_clientes_rfm | dim_sellers_score\nfato_vendas_diarias | 1 ano", "#F9FBE7", "#558B2F", fontsize=9.5, bold=True)

# ─── CLUSTER: PROCESSAMENTO ─────────────────────────────────────────────────
cluster(ax, 4.5, 5.0, 4.2, 3.9, "④ Processamento", "#E3F2FD", "#1565C0")
box(ax, 4.7, 7.2, 3.8, 1.3, "Apache Spark",
    "PySpark Notebooks\nAQE + Broadcast Join", "#E3F2FD", "#1565C0", bold=True)
box(ax, 4.7, 5.3, 3.8, 1.7, "Databricks Workspace",
    "All-Purpose Cluster (dev)\nJobs Cluster (prod)\nAuto-scaling | Spot Instances", "#BBDEFB", "#1565C0")

# ─── CLUSTER: CONSUMO ───────────────────────────────────────────────────────
cluster(ax, 15.1, 7.5, 6.5, 5.0, "⑤ Consumo Analítico", "#FFF8E1", "#F57F17")
box(ax, 15.4, 10.3, 5.9, 1.8, "Amazon Redshift Serverless",
    "Queries SQL de alta performance\nPay-per-query | Sem cluster ocioso", "#FFF3E0", "#E65100", bold=True)
box(ax, 15.4, 8.2,  5.9, 1.8, "Databricks SQL Warehouse",
    "Consultas ad-hoc\nNotebooks de análise exploratória", "#E3F2FD", "#1565C0", bold=True)
box(ax, 15.4, 7.8,  2.8, 0.2, "Power BI / Tableau", "", "#FFFDE7", "#F9A825", fontsize=8)

# ─── CLUSTER: GOVERNANÇA ────────────────────────────────────────────────────
cluster(ax, 0.3, 2.0, 21.0, 2.8, "⑥ Governança e Segurança (Transversal — LGPD)", "#FCE4EC", "#C62828")
box(ax, 0.6,  2.3, 4.5, 1.8, "AWS Lake Formation",
    "Row/Column-level security\nPermissões por perfil\nAudit logs (LGPD)", "#FCE4EC", "#C62828")
box(ax, 5.5,  2.3, 4.5, 1.8, "AWS Glue Data Catalog",
    "Metadados e schemas\nLinhagem de dados\nPartições por data", "#FCE4EC", "#C62828")
box(ax, 10.4, 2.3, 4.5, 1.8, "AWS IAM + KMS",
    "Autenticação/Autorização\nCriptografia SSE-KMS\nRoles por equipe", "#FCE4EC", "#C62828")
box(ax, 15.3, 2.3, 5.7, 1.8, "Amazon MWAA (Airflow)",
    "Orquestração de pipelines\nDAGs agendadas\nAlertas via SNS (falhas)", "#FCE4EC", "#C62828")

# ─── CLUSTER: ORQUESTRAÇÃO nota ────────────────────────────────────────────
ax.text(11, 1.55, "* Orquestração (Airflow) e modelos de ML (predição de churn, detecção de fraude) serão implementados nos TPs seguintes.",
        ha="center", va="center", fontsize=8, color="#777777", style="italic")

# ─── SETAS PRINCIPAIS ────────────────────────────────────────────────────────
# Fontes → Glue
arrow(ax, 4.1, 11.7, 4.7, 11.7, "#4CAF50")
# Fontes → Raw
arrow(ax, 4.1, 10.5, 9.4, 11.9, "#7B1FA2")
# Glue → Bronze
arrow(ax, 8.5, 11.7, 9.4, 10.5, "#4CAF50")
# Databricks ingest → Bronze
arrow(ax, 8.5, 10.2, 9.4, 10.2, "#1565C0")
# Bronze → Spark
arrow(ax, 9.2, 9.8, 8.5, 8.0, "#1565C0")
# Spark → Silver
arrow(ax, 8.5, 7.5, 9.4, 8.7, "#00838F")
# Silver → Gold
arrow(ax, 11.7, 8.0, 11.7, 7.7, "#558B2F")
# Gold → Redshift
arrow(ax, 14.0, 6.9, 15.4, 11.0, "#E65100")
# Gold → Databricks SQL
arrow(ax, 14.0, 6.9, 15.4, 9.1, "#1565C0")

# Governança (tracejado) conectando ao S3
for y_pos in [10.5, 8.7, 6.9]:
    arrow_dash(ax, 5.0, 3.3, 9.4, y_pos, "#C62828")

plt.tight_layout(pad=0.5)
plt.savefig("docs/tp1/arquitetura_brasilmart.png", dpi=150, bbox_inches="tight",
            facecolor="#F8F9FA")
print("Diagrama salvo em: docs/tp1/arquitetura_brasilmart.png")
