#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Configuración de PySpark para mac m1
import os
import sys

# Configurar Java y Spark  local
os.environ['JAVA_HOME'] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
os.environ['SPARK_HOME'] = '/opt/homebrew/opt/apache-spark/libexec'
os.environ['PYSPARK_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd
import json

spark = SparkSession.builder \
    .appName("CustomerDataAnalysis") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

print("Sesión de Spark inicializada")


# In[3]:


file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "customer.csv")
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(file_path)

df.createOrReplaceTempView("customers")

print(f"Número total de registros: {df.count()}")
print("\nEsquema del DataFrame:")
df.printSchema()

print("\nPrimeros 5 registros:")
df.show(5, truncate=False)


# In[7]:


country_distribution = spark.sql("""
    SELECT 
        COALESCE(country, 'Sin país') as country, 
        COUNT(*) as user_count
    FROM customers
    GROUP BY COALESCE(country, 'Sin país')
    ORDER BY user_count DESC
""")

print("Distribución de usuarios por país (incluyendo nulos):")
country_distribution.show(10)

country_pd = country_distribution.toPandas()
plt.figure(figsize=(10, 6))
plt.bar(country_pd['country'], country_pd['user_count'])
plt.title('Distribución de Usuarios por País')
plt.xlabel('País')
plt.ylabel('Número de Usuarios')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# In[6]:


status_distribution = spark.sql("""
    SELECT 
        status, 
        COUNT(*) as count
    FROM customers
    GROUP BY status
    ORDER BY count DESC
""")

print("Distribución por estado de cuenta:")
status_distribution.show()

status_pd = status_distribution.toPandas()
plt.figure(figsize=(10, 6))
plt.pie(status_pd['count'], labels=status_pd['status'], autopct='%1.1f%%')
plt.title('Distribución de Estados de Cuenta')
plt.tight_layout()
plt.show()


# In[8]:


deposit_by_time = spark.sql("""
    SELECT 
        date_format(fiat_first_deposit_created_at, 'yyyy-MM') as month,
        COUNT(*) as deposit_count,
        SUM(fiat_first_deposit_amount) as total_deposit_amount,
        AVG(fiat_first_deposit_amount) as avg_deposit_amount
    FROM customers
    WHERE fiat_first_deposit_created_at IS NOT NULL
    GROUP BY date_format(fiat_first_deposit_created_at, 'yyyy-MM')
    ORDER BY month
""")

print("Depósitos por mes:")
deposit_by_time.show(20)

deposit_pd = deposit_by_time.toPandas()
plt.figure(figsize=(12, 6))
plt.bar(deposit_pd['month'], deposit_pd['total_deposit_amount'])
plt.title('Monto total de depósitos por mes')
plt.xlabel('Mes')
plt.ylabel('Monto total ($)')
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()


# In[9]:


investment_analysis = spark.sql("""
    SELECT 
        investment_objective,
        investment_experience,
        COUNT(*) as user_count
    FROM customers
    WHERE investment_objective IS NOT NULL
      AND investment_experience IS NOT NULL
    GROUP BY investment_objective, investment_experience
    ORDER BY user_count DESC
""")

print("Análisis de experiencia vs objetivo de inversión:")
investment_analysis.show()

inv_pd = investment_analysis.toPandas()

pivot_table = inv_pd.pivot(index='investment_objective', 
                          columns='investment_experience', 
                          values='user_count').fillna(0)

plt.figure(figsize=(12, 8))
ax = plt.subplot(111)
im = ax.imshow(pivot_table.values, cmap='YlGnBu')

ax.set_xticks(range(len(pivot_table.columns)))
ax.set_yticks(range(len(pivot_table.index)))
ax.set_xticklabels(pivot_table.columns)
ax.set_yticklabels(pivot_table.index)
plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

for i in range(len(pivot_table.index)):
    for j in range(len(pivot_table.columns)):
        text = ax.text(j, i, int(pivot_table.iloc[i, j]),
                       ha="center", va="center", color="black")

plt.colorbar(im)
plt.title('Número de usuarios por Objetivo y Experiencia de Inversión')
plt.tight_layout()
plt.show()


# In[10]:


deposit_amount_analysis = spark.sql("""
    SELECT 
        CASE
            WHEN fiat_first_deposit_amount < 50 THEN 'Menos de 50'
            WHEN fiat_first_deposit_amount >= 50 AND fiat_first_deposit_amount < 100 THEN '50-100'
            WHEN fiat_first_deposit_amount >= 100 AND fiat_first_deposit_amount < 500 THEN '100-500'
            WHEN fiat_first_deposit_amount >= 500 AND fiat_first_deposit_amount < 1000 THEN '500-1000'
            ELSE 'Más de 1000'
        END as deposit_range,
        COUNT(*) as user_count,
        AVG(fiat_first_deposit_amount) as avg_amount
    FROM customers
    WHERE fiat_first_deposit_amount IS NOT NULL
    GROUP BY 
        CASE
            WHEN fiat_first_deposit_amount < 50 THEN 'Menos de 50'
            WHEN fiat_first_deposit_amount >= 50 AND fiat_first_deposit_amount < 100 THEN '50-100'
            WHEN fiat_first_deposit_amount >= 100 AND fiat_first_deposit_amount < 500 THEN '100-500'
            WHEN fiat_first_deposit_amount >= 500 AND fiat_first_deposit_amount < 1000 THEN '500-1000'
            ELSE 'Más de 1000'
        END
    ORDER BY 
        CASE 
            WHEN deposit_range = 'Menos de 50' THEN 1
            WHEN deposit_range = '50-100' THEN 2
            WHEN deposit_range = '100-500' THEN 3
            WHEN deposit_range = '500-1000' THEN 4
            ELSE 5
        END
""")

print("Análisis de montos de primer depósito:")
deposit_amount_analysis.show()

deposit_range_pd = deposit_amount_analysis.toPandas()
plt.figure(figsize=(12, 6))
plt.bar(deposit_range_pd['deposit_range'], deposit_range_pd['user_count'])
plt.title('Distribución de Montos de Primer Depósito')
plt.xlabel('Rango de Depósito')
plt.ylabel('Número de Usuarios')
plt.tight_layout()
plt.show()


# In[5]:


# Análisis manual del JSON (sin cargarlo en Spark para evitar problemas de memoria)
import json

json_path = "/Users/nicolasasparriayara/Desktop/balance_report_2025-02-28.json"

# Cargar solo las claves principales
with open(json_path, 'r') as file:
    balance_data = json.load(file)

# Ver las claves principales
print("Claves principales del reporte de balance:")
for key in balance_data.keys():
    print(f"- {key}")

# Mostrar algunos valores principales
print("\nValores principales:")
for key in balance_data.keys():
    if isinstance(balance_data[key], (int, float, str)):
        print(f"{key}: {balance_data[key]}")
    elif isinstance(balance_data[key], dict) and len(balance_data[key]) < 5:
        print(f"{key}: {balance_data[key]}")
    else:
        print(f"{key}: [Datos complejos]")

# Crear una visualización simple de los datos principales si hay valores numéricos
numeric_values = {}
for key in balance_data.keys():
    if isinstance(balance_data[key], (int, float)):
        numeric_values[key] = balance_data[key]

if numeric_values:
    plt.figure(figsize=(10, 6))
    plt.bar(numeric_values.keys(), numeric_values.values())
    plt.title('Métricas Principales de Balance')
    plt.ylabel('Valor')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# In[12]:


import time

print("Simulando correr la query en este entorno...")
start_time = time.time()

complex_query_result = spark.sql("""
    SELECT 
        c.country,
        YEAR(c.fiat_first_deposit_created_at) as year,
        MONTH(c.fiat_first_deposit_created_at) as month,
        COUNT(DISTINCT c.user_id) as unique_users,
        SUM(c.fiat_first_deposit_amount) as total_deposit,
        AVG(c.fiat_first_deposit_amount) as avg_deposit,
        COUNT(CASE WHEN c.is_investor = 'true' THEN 1 END) as investor_count,
        COUNT(CASE WHEN c.is_trader = 'true' THEN 1 END) as trader_count
    FROM customers c
    WHERE 
        c.fiat_first_deposit_created_at IS NOT NULL
        AND c.status = 'ONBOARD_COMPLETED'
    GROUP BY 
        c.country,
        YEAR(c.fiat_first_deposit_created_at),
        MONTH(c.fiat_first_deposit_created_at)
    ORDER BY 
        year DESC, 
        month DESC, 
        total_deposit DESC
""")

complex_query_result.count()
spark_time = time.time() - start_time

print(f"Tiempo de ejecución en Spark: {spark_time:.2f} segundos")

print("\nResultados:")
complex_query_result.show(10)


# In[1]:


# Demostración de integración con PostgreSQL (solo código de ejm)
print("""
# Código para integración con PostgreSQL RDS en producción:

# 1. Lectura desde PostgreSQL:
df_from_pg = spark.read \\
    .format("jdbc") \\
    .option("url", "your_db") \\
    .option("dbtable", "schema.table_name") \\
    .option("user", "root") \\
    .option("password", "your_password") \\
    .option("driver", "org.postgresql.Driver") \\
    .load()

# 2. Escritura a PostgreSQL:
complex_query_result.write \\
    .format("jdbc") \\
    .option("url", "your_db") \\
    .option("dbtable", "analytics.monthly_metrics") \\
    .option("user", "root") \\
    .option("password", "your_password") \\
    .option("driver", "org.postgresql.Driver") \\
    .mode("append") \\
    .save()
""")

