# Databricks notebook source
import urllib.request
import json
import pandas as pd
from pyspark.sql.functions import col, current_timestamp

# 1. Pobieramy dane z GCP
url = "https://storage.googleapis.com/raw-crypto-data-bucket-123/raw_crypto_data.json"
response = urllib.request.urlopen(url)
data = json.loads(response.read().decode())

# 2. Konwertujemy najpierw do Pandas DataFrame, aby ujednolicić typy liczbowe
pandas_df = pd.DataFrame(data)
raw_df = spark.createDataFrame(pandas_df)

# 3. Transformacja do Warstwy SILVER:
silver_df = raw_df.select(
    col("id").alias("coin_id"),
    col("symbol"),
    col("name"),
    col("current_price").cast("double"),
    col("market_cap").cast("double"),
    col("total_volume").cast("double"),
    col("price_change_percentage_24h").alias("change_24h_pct").cast("double")
).filter(col("current_price").isNotNull()) \
 .withColumn("ingested_at", current_timestamp())

# Zapisujemy wynik jako tabelę w formacie Delta Lake (Warstwa SILVER)
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_crypto_markets")

# Wyświetlamy podgląd warstwy Silver
display(silver_df)

# COMMAND ----------

from pyspark.sql.functions import avg, max, min, sum, count

# 1. Tworzenie tabeli zagregowanej (Gold Layer - Market Summary)
# Grupowanie danych biznesowych pod kątem statystyk i analityki
gold_summary_df = spark.sql("""
    SELECT 
        UPPER(symbol) as symbol,
        name,
        current_price,
        market_cap,
        total_volume,
        change_24h_pct,
        CASE 
            WHEN change_24h_pct > 0 THEN 'BULLISH'
            WHEN change_24h_pct < 0 THEN 'BEARISH'
            ELSE 'NEUTRAL'
        END as market_sentiment
    FROM silver_crypto_markets
""")

# 2. Zapis do warstwy GOLD jako osobna tabela Delta Lake
gold_summary_df.write.format("delta").mode("overwrite").saveAsTable("gold_crypto_summary")

# 3. Wyświetlenie wyników
display(gold_summary_df)

# COMMAND ----------

import requests

def ai_query_engine(user_prompt: str, api_key: str = None):
    """
    Generuje zapytanie SQL na podstawie języka naturalnego przy użyciu LLM (Llama-3 przez Groq API).
    """
    print(f"💬 Pytanie użytkownika: '{user_prompt}'")
    
    schema_info = """
    Table: gold_crypto_summary
    Columns:
    - symbol (STRING)
    - name (STRING)
    - current_price (DOUBLE)
    - market_cap (DOUBLE)
    - change_24h_pct (DOUBLE)
    - market_sentiment (STRING: 'BULLISH' or 'BEARISH')
    """
    
    # Jeśli brak klucza API, stosujemy fallback z promptem systemowym
    if not api_key:
        print("Brak API Key - symulacja strukturalna promptu systemowego LLM.")
        prompt_lower = user_prompt.lower()
        if "spadły" in prompt_lower or "bearish" in prompt_lower:
            generated_sql = "SELECT name, symbol, change_24h_pct FROM gold_crypto_summary WHERE market_sentiment = 'BEARISH' ORDER BY change_24h_pct ASC"
        else:
            generated_sql = "SELECT name, symbol, market_cap FROM gold_crypto_summary ORDER BY market_cap DESC LIMIT 5"
        return spark.sql(generated_sql)

    # Zapytanie do prawdziwego LLM (Groq / Llama-3)
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": "llama3-8b-8192",
        "messages": [
            {"role": "system", "content": f"You are a SQL expert. Translate user prompt into Spark SQL based ONLY on this schema: {schema_info}. Return ONLY the raw SQL query without markdown code blocks."},
            {"role": "user", "content": user_prompt}
        ]
    }
    
    response = requests.post(url, json=payload, headers=headers)
    generated_sql = response.json()['choices'][0]['message']['content'].strip()
    
    print(f"🤖 Wygenerowany przez LLM kod SQL:\n{generated_sql}\n")
    return spark.sql(generated_sql)

# COMMAND ----------

ai_query_engine("Pokaż mi krypto, które najbardziej spadły")

# COMMAND ----------

display(ai_query_engine("Pokaż mi krypto, które najbardziej spadły"))

# COMMAND ----------

