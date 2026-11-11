import os
import json
import time
import uuid

import pandas as pd
import psycopg2
import streamlit as st
from kafka import KafkaProducer


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")


def load_file(uploaded_file):
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Не удалось прочитать файл: {e}")
        return None


def send_to_kafka(df, topic, bootstrap_servers):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    df["transaction_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    progress = st.progress(0)
    n = len(df)

    for i, row in df.iterrows():
        producer.send(
            topic,
            value={
                "transaction_id": row["transaction_id"],
                "data": row.drop("transaction_id").to_dict(),
            },
        )
        progress.progress((i + 1) / n)
        time.sleep(0.01)

    producer.flush()


st.title("Fraud Detection Service")

st.header("Загрузка транзакций")

if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

uploaded_file = st.file_uploader("CSV файл с транзакциями", type=["csv"])

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "загружен",
        "df": load_file(uploaded_file),
    }
    st.success(f"Файл {uploaded_file.name} загружен")

if st.session_state.uploaded_files:
    st.subheader("Файлы")
    for name, data in st.session_state.uploaded_files.items():
        c1, c2, c3 = st.columns([4, 2, 2])
        c1.write(f"**{name}**")
        c1.write(f"Статус: `{data['status']}`")

        if c3.button("Отправить", key=f"btn_{name}"):
            if data["df"] is not None:
                send_to_kafka(data["df"], KAFKA_TOPIC, KAFKA_BROKERS)
                st.session_state.uploaded_files[name]["status"] = "отправлен"
                st.rerun()


st.header("Результаты")

if st.button("Посмотреть результаты"):
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "postgres"),
            port=os.getenv("DB_PORT", "5432"),
            dbname=os.getenv("DB_NAME", "fraud_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
        )

        st.subheader("Последние фроды (fraud_flag = 1)")
        df_fraud = pd.read_sql(
            "SELECT transaction_id, score, fraud_flag, model_version, created_at "
            "FROM scores WHERE fraud_flag = 1 "
            "ORDER BY created_at DESC LIMIT 10",
            conn,
        )
        if df_fraud.empty:
            st.info("Фродов пока нет")
        else:
            st.dataframe(df_fraud, use_container_width=True)

        st.subheader("Распределение скоров (последние 100)")
        df_scores = pd.read_sql(
            "SELECT score FROM scores ORDER BY created_at DESC LIMIT 100",
            conn,
        )
        if df_scores.empty:
            st.info("Транзакций пока нет")
        else:
            st.bar_chart(df_scores["score"].value_counts(bins=20).sort_index())

        conn.close()
    except Exception as e:
        st.error(f"Не удалось получить данные: {e}")