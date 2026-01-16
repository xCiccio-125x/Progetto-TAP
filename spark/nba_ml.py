from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql.window import Window

spark = (SparkSession.builder
    .appName("NBA_RealTime_Prediction")
    .master("local[2]")                       
    .config("spark.sql.shuffle.partitions", "2") 
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

print("Caricamento Modelli...")
try:
    model_win = PipelineModel.load("/app/pipeline_data/models/win_model")
    model_mvp = PipelineModel.load("/app/pipeline_data/models/mvp_model")
    print("Modelli caricati correttamente!")
except Exception as e:
    print(f"Errore caricamento modelli: {e}")

# --- schema di input ---
nba_schema = StructType([
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
    StructField("Posizioni", ArrayType(StructType([
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("r", IntegerType(), True) ])), True)
])

# --- leggo da kafka ---
data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "nba-kafka:9092") \
    .option("subscribe", "nba_aggregation") \
    .option("startingOffsets", "earliest") \
    .load()

json_data = data.select(F.from_json(F.col("value").cast("string"), nba_schema).alias("data")).select("data.*").na.fill(0).na.fill(0.0)

def process_ml_batch(batch_df, batch_id):
    
    if batch_df.count() == 0:
        return 
        
    batch_df.persist()  
    
    batch_df = batch_df.dropDuplicates(['Partita', 'Giocatore', 'Seconds_Remaining'])
    
    # --- PREDIZIONE VITTORIA ---
        
    win = model_win.transform(batch_df)
    win = win.withColumn("Win%", vector_to_array(F.col("probability"))[1]) \
                .drop("rawPrediction", "probability", "prediction", "features")
                    
    # --- PREDIZIONE MVP ---
        
    # Normalizzo Efficiency 
    window_Squadra = Window.partitionBy("Partita", "Squadra", "Seconds_Remaining")
    win = win.withColumn("Total_Eff_Team", F.sum("Efficiency").over(window_Squadra)) \
                    .withColumn("Efficiency_Normalized", F.when(F.col("Total_Eff_Team") > 0, F.col("Efficiency") / F.col("Total_Eff_Team")).otherwise(0.0)) \
                    .drop("Total_Eff_Team")
        
    mvp = model_mvp.transform(win)
    mvp = mvp.withColumn("MVP_Prob", vector_to_array(F.col("probability"))[1]) \
                        .drop("Efficiency_Normalized", "rawPrediction", "probability", "prediction", "features")
    
    # Normalizzo MVP_Prob
    window_partita = Window.partitionBy("Partita", "Seconds_Remaining")
    dati_completi = mvp.withColumn("Total_MVP_Prob", F.sum("MVP_Prob").over(window_partita)) \
                        .withColumn("MVP%", F.when(F.col("Total_MVP_Prob") > 0, (F.col("MVP_Prob") / F.col("Total_MVP_Prob")) * 100).otherwise(0.0)) \
                        .drop("Total_MVP_Prob","MVP_Prob")

    # Prima di inviare aggiungo il timestamp per kibana
    dati_completi = dati_completi.withColumn("@timestamp", F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
        
    # --- D. OUTPUT FINALE ---
    output = dati_completi.select(
        F.col("@timestamp"), 
        F.col("Partita"),   
        F.col("Squadra"),   
        F.col("Punteggio_Squadra"),  
        F.col("Score_Diff"),    
        F.col("Seconds_Remaining"), 
        F.col("Quarto_globale"), 
        F.col("Clock_globale"), 
        F.col("Giocatore"), 
        F.col("Punti"), 
        F.col("Assist"), 
        F.col("Rimb"), 
        F.col("Palla_r"), 
        F.col("Stopp"), 
        F.col("Palla_p"), 
        F.col("Falli"), 
        F.col("%_Tiro"), 
        F.col("Efficiency"),  
        F.col("Posizioni"), 
        F.format_number(F.col("Win%") * 100, 2).cast("float").alias("Win%"),    
        F.format_number(F.col("MVP%"), 2).cast("float").alias("MVP%")       
    )
                    
    print("Predico e invio a Elastic...")
    output.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "nba-elastic-search") \
        .option("es.port", "9200") \
        .option("es.resource", "nba_game_data") \
        .option("es.nodes.wan.only", "true") \
        .mode("append") \
        .save()
        
    batch_df.unpersist()   

query = json_data.writeStream \
    .outputMode("append") \
    .foreachBatch(process_ml_batch) \
    .trigger(processingTime='3 seconds') \
    .start()

query.awaitTermination()