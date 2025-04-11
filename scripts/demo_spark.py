from pyspark.sql import SparkSession
import findspark
import os
import sys
from datetime import datetime

findspark.init()

def create_spark_session():
    return (SparkSession.builder
            .appName("CustomerDataAnalysis")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())

def main():
    print("Iniciando sesión de Spark...")
    spark = create_spark_session()
    
    file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "data", "customer.csv")
    
    print(f"Leyendo datos desde: {file_path}")
    
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        
        print("\nEsquema del DataFrame:")
        df.printSchema()
        
        print("\nPrimeros 10 registros:")
        df.show(10, truncate=False)
        
        df.createOrReplaceTempView("customers")
        
        print("\nAnálisis básico de datos:")
        

        try:
            count_query = """
                SELECT 
                    COUNT(*) as total_customers
                FROM customers
            """
            
            spark.sql(count_query).show()
            
            
        except Exception as e:
            print(f"Error en consultas SQL: {e}")
            
        print("\nGuardando resultados de muestra...")
        sample_output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                         "data", "sample_output")
        
        df.sample(fraction=0.1).write.mode("overwrite").csv(sample_output_path)
        
        print(f"Resultados guardados en: {sample_output_path}")
        
    except Exception as e:
        print(f"Error al procesar el archivo: {e}")
        sys.exit(1)
    
    spark.stop()
    print("Sesión de Spark finalizada.")

if __name__ == "__main__":
    start_time = datetime.now()
    main()
    end_time = datetime.now()
    print(f"Tiempo de ejecución: {end_time - start_time}")