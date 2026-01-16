from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# --- CONFIGURAZIONE ---
KAFKA_BOOTSTRAP = "nba-kafka:9092"
INPUT_TOPIC = "nba_aggregation" 
OUTPUT_PATH = "/tmp/training_dataset"
CHECKPOINT = "/tmp/checkpoints/saver"

spark = SparkSession.builder.appName("NBA_Data_Recorder").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Schema di input
schema = StructType([
    StructField("Partita", StringType(), True),
    StructField("Squadra", StringType(), True),
    StructField("Punteggio_Squadra", IntegerType(), True),
    StructField("Score_Diff", IntegerType(), True),       
    StructField("Seconds_Remaining", IntegerType(), True),
    StructField("Quarto_globale", IntegerType(), True),
    StructField("Clock_globale", StringType(), True),
    StructField("Giocatore", StringType(), True),
    StructField("Punti", IntegerType(), True),
    StructField("Assist", IntegerType(), True),
    StructField("Rimb", IntegerType(), True),
    StructField("Palla_r", IntegerType(), True),
    StructField("Stopp", IntegerType(), True),
    StructField("Palla_p", IntegerType(), True),
    StructField("Falli", IntegerType(), True),
    StructField("%_Tiro", DoubleType(), True),
    StructField("Efficiency", DoubleType(), True),
    StructField("Posizioni", StringType(), True) 
])

print("In attesa di dati da Spark...", flush=True)

# legge da Kafka
data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", INPUT_TOPIC) \
    .load()
data = data.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Salva su Disco (Parquet)
query = data.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT) \
    .partitionBy("Partita") \
    .start()

try:
    while query.isActive:
        query.awaitTermination(5)      
        progress = query.lastProgress
        if progress:
            num_rows = progress['numInputRows']
            batch_id = progress['batchId']
            if num_rows > 0:
                print(f"Batch {batch_id}: Scritte {num_rows} righe.", flush=True)
            else:
                print(f"In attesa dati...", flush=True)
except KeyboardInterrupt:
    query.stop()