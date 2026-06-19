"""
Preparação do JSON Aninhado — Pedido Unificado BrasilMart

Combina 4 CSVs da Olist em documentos JSON aninhados (1 por pedido):
  - olist_orders_dataset.csv       → base do documento
  - olist_customers_dataset.csv    → objeto customer{}
  - olist_order_items_dataset.csv  → array items[]
  - olist_order_payments_dataset.csv → array payments[]
  - olist_order_reviews_dataset.csv  → objeto review{}

O JSON resultante simula uma resposta de API de e-commerce e
é a entrada do Notebook 01 (ingestão de dados complexos no Databricks).

Uso:
    python data/prepare_json_orders.py

Saída:
    data/raw/orders_json/orders_unified.jsonl   (JSON Lines, 1 pedido por linha)
    data/raw/orders_json/streaming/             (arquivos por dia, para simular streaming)
"""

import json
import os
from pathlib import Path

import pandas as pd

RAW_DIR = Path(__file__).parent / "raw"
OUTPUT_DIR = Path(__file__).parent / "raw" / "orders_json"
STREAMING_DIR = OUTPUT_DIR / "streaming"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
STREAMING_DIR.mkdir(parents=True, exist_ok=True)


def load_datasets():
    print("Carregando datasets Olist...")

    orders = pd.read_csv(
        RAW_DIR / "olist_orders_dataset.csv",
        parse_dates=[
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )
    customers = pd.read_csv(RAW_DIR / "olist_customers_dataset.csv")
    items = pd.read_csv(
        RAW_DIR / "olist_order_items_dataset.csv",
        parse_dates=["shipping_limit_date"],
    )
    payments = pd.read_csv(RAW_DIR / "olist_order_payments_dataset.csv")
    reviews = pd.read_csv(
        RAW_DIR / "olist_order_reviews_dataset.csv",
        parse_dates=["review_creation_date", "review_answer_timestamp"],
    )

    print(f"  orders:   {len(orders):,}")
    print(f"  customers: {len(customers):,}")
    print(f"  items:    {len(items):,}")
    print(f"  payments: {len(payments):,}")
    print(f"  reviews:  {len(reviews):,}")

    return orders, customers, items, payments, reviews


def to_isoformat(val):
    """Converte Timestamp pandas para string ISO, None se NaT."""
    if pd.isna(val):
        return None
    return str(val)


def build_unified_order(row, cust, order_items, order_payments, order_review):
    doc = {
        "order_id": row["order_id"],
        "order_status": row["order_status"],
        "customer": {
            "customer_id": row["customer_id"],
            "customer_unique_id": cust.get("customer_unique_id"),
            "zip_code_prefix": cust.get("customer_zip_code_prefix"),
            "city": cust.get("customer_city"),
            "state": cust.get("customer_state"),
        },
        "timestamps": {
            "purchase": to_isoformat(row["order_purchase_timestamp"]),
            "approved": to_isoformat(row["order_approved_at"]),
            "delivered_carrier": to_isoformat(row["order_delivered_carrier_date"]),
            "delivered_customer": to_isoformat(row["order_delivered_customer_date"]),
            "estimated_delivery": to_isoformat(row["order_estimated_delivery_date"]),
        },
        "items": [
            {
                "order_item_id": int(item["order_item_id"]),
                "product_id": item["product_id"],
                "seller_id": item["seller_id"],
                "shipping_limit_date": to_isoformat(item["shipping_limit_date"]),
                "price": float(item["price"]),
                "freight_value": float(item["freight_value"]),
            }
            for _, item in order_items.iterrows()
        ],
        "payments": [
            {
                "sequential": int(pmt["payment_sequential"]),
                "type": pmt["payment_type"],
                "installments": int(pmt["payment_installments"]),
                "value": float(pmt["payment_value"]),
            }
            for _, pmt in order_payments.iterrows()
        ],
        "review": None,
    }

    if order_review is not None:
        doc["review"] = {
            "review_id": order_review.get("review_id"),
            "score": int(order_review["review_score"]) if pd.notna(order_review.get("review_score")) else None,
            "comment_title": order_review.get("review_comment_title") if pd.notna(order_review.get("review_comment_title")) else None,
            "comment_message": order_review.get("review_comment_message") if pd.notna(order_review.get("review_comment_message")) else None,
            "creation_date": to_isoformat(order_review.get("review_creation_date")),
            "answer_timestamp": to_isoformat(order_review.get("review_answer_timestamp")),
        }

    return doc


def generate_unified_json(orders, customers, items, payments, reviews):
    print("\nConstruindo JSON unificado por pedido...")

    customers_idx = customers.set_index("customer_id").to_dict("index")
    items_idx = items.groupby("order_id")
    payments_idx = payments.groupby("order_id")
    reviews_idx = reviews.drop_duplicates("order_id").set_index("order_id")

    unified_path = OUTPUT_DIR / "orders_unified.jsonl"
    total = 0

    with open(unified_path, "w", encoding="utf-8") as f_out:
        for _, row in orders.iterrows():
            oid = row["order_id"]
            cid = row["customer_id"]

            cust = customers_idx.get(cid, {})
            order_items = items_idx.get_group(oid) if oid in items_idx.groups else pd.DataFrame()
            order_payments = payments_idx.get_group(oid) if oid in payments_idx.groups else pd.DataFrame()
            order_review = reviews_idx.loc[oid] if oid in reviews_idx.index else None

            doc = build_unified_order(row, cust, order_items, order_payments, order_review)
            f_out.write(json.dumps(doc, ensure_ascii=False) + "\n")
            total += 1

            if total % 10000 == 0:
                print(f"  {total:,} pedidos processados...")

    print(f"  ✓ {total:,} documentos JSON → {unified_path}")
    return unified_path


def generate_streaming_partitions(orders, unified_path):
    """
    Divide o JSON unificado em arquivos por dia (simula chegada incremental de pedidos).
    Cada arquivo representa 1 dia de novos pedidos — usado no Notebook 02 (streaming).
    """
    print("\nGerando partições diárias para simulação de streaming...")

    orders_by_date = orders.groupby(orders["order_purchase_timestamp"].dt.date)

    # Carregar documentos gerados
    docs_by_id = {}
    with open(unified_path, "r", encoding="utf-8") as f:
        for line in f:
            doc = json.loads(line)
            docs_by_id[doc["order_id"]] = doc

    partitions_created = 0
    for date, group in orders_by_date:
        date_str = str(date)
        date_dir = STREAMING_DIR / date_str
        date_dir.mkdir(exist_ok=True)
        out_file = date_dir / "orders.jsonl"

        with open(out_file, "w", encoding="utf-8") as f:
            for oid in group["order_id"]:
                if oid in docs_by_id:
                    f.write(json.dumps(docs_by_id[oid], ensure_ascii=False) + "\n")

        partitions_created += 1

    print(f"  ✓ {partitions_created} partições diárias → {STREAMING_DIR}/")


def main():
    print("=" * 60)
    print("BrasilMart — Preparação de JSON Aninhado")
    print("=" * 60)

    orders, customers, items, payments, reviews = load_datasets()
    unified_path = generate_unified_json(orders, customers, items, payments, reviews)
    generate_streaming_partitions(orders, unified_path)

    print("\nArquivos gerados:")
    print(f"  {OUTPUT_DIR / 'orders_unified.jsonl'}  (todos os pedidos, 1 por linha)")
    print(f"  {STREAMING_DIR}/YYYY-MM-DD/orders.jsonl  (particionado por dia)")
    print("\nPara upload ao S3:")
    print("  aws s3 cp data/raw/orders_json/ s3://brasilmart-raw-dev/orders_json/ --recursive")


if __name__ == "__main__":
    main()
