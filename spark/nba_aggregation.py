from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, when, round, struct, array, explode, lit, max, to_json, collect_list, split, coalesce, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window

# --- CONFIGURAZIONE ---
KAFKA_BOOTSTRAP_SERVERS = "nba-kafka:9092"
KAFKA_TOPIC = "nba_events"
OUTPUT_TOPIC_ML = "nba_aggregation"

spark = (SparkSession.builder 
    .appName("NBA RealTime Stats") 
    .master("local[2]")                    
    .config("spark.sql.shuffle.partitions", "2") 
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") 
    .getOrCreate())
    
spark.sparkContext.setLogLevel("ERROR")

# definisco lo schema di input
schema = StructType([
    StructField("game_id", StringType(), True),
    StructField("event_id", IntegerType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("team_code", StringType(), True),
    StructField("player_name", StringType(), True),
    StructField("event_type", StringType(), True),    
    StructField("shot_result", StringType(), True),    
    StructField("shot_value", IntegerType(), True),    
    StructField("assist_player", StringType(), True),
    StructField("clock", StringType(), True),
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True)
])

# --- LETTURA KAFKA ---
data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 30) \
    .load()

data = data.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
data = data.filter(col("player_name").isNotNull())

# --- PREPARAZIONE DATI ---

# Struttura per il giocatore principale
main_player_metrics = struct(
    col("player_name").alias("p_name"),
    col("event_id").alias("evt_id"),  
    col("quarter").alias("qtr"), 
    col("clock").alias("clk"),  
    when((col("shot_result").isin("Made", "Missed")) & (col("event_type") != "freethrow"), col("x")).otherwise(None).alias("x"),
    when((col("shot_result").isin("Made", "Missed")) & (col("event_type") != "freethrow"), col("y")).otherwise(None).alias("y"),
    when(col("shot_result") == "Made", col("shot_value")).otherwise(0).alias("pts"),
    when(col("event_type") == "rebound", 1).otherwise(0).alias("reb"),
    when(col("event_type") == "steal", 1).otherwise(0).alias("stl"),
    when(col("event_type") == "block", 1).otherwise(0).alias("blk"),
    when(col("event_type") == "turnover", 1).otherwise(0).alias("to"),
    when(col("event_type") == "foul", 1).otherwise(0).alias("pf"),
    when((col("shot_result").isin("Made", "Missed")) & (col("event_type") != "freethrow"), 1).otherwise(0).alias("fga"),
    when((col("shot_result") == "Made") & (col("event_type") != "freethrow"), 1).otherwise(0).alias("fgm"),
    lit(0).alias("ast")         #creo una colonna che vale sempre 0
)

# Struttura per il giocatore che effettua l'assist
assist_player_metrics = struct(
    col("assist_player").alias("p_name"),
    col("event_id").alias("evt_id"),
    col("quarter").alias("qtr"),
    col("clock").alias("clk"),
    lit(None).alias("x"), 
    lit(None).alias("y"),
    lit(0).alias("pts"),
    lit(0).alias("reb"),
    lit(0).alias("stl"),
    lit(0).alias("blk"),
    lit(0).alias("to"),
    lit(0).alias("pf"),
    lit(0).alias("fga"),
    lit(0).alias("fgm"),
    lit(1).alias("ast")
)

# Uso explode sulle due strutture: una singola riga di input diventa due righe di output. 
# Così Spark può calcolare le statistiche per entrambi i giocatori contemporaneamente.
data_exploded = data.select(
    col("game_id"), 
    col("team_code"),
    explode(array(main_player_metrics, assist_player_metrics)).alias("stats")
)

#Se il player_name non è vuoto avrò il doppio delle righe
data_ready = data_exploded.select(
    col("game_id"),
    col("team_code"),
    col("stats.p_name").alias("player_name"),
    col("stats.evt_id").alias("event_ID"), 
    col("stats.qtr").alias("quarter"),
    col("stats.clk").alias("clock"),
    col("stats.x").alias("shot_X"),
    col("stats.y").alias("shot_Y"),
    col("stats.pts").alias("is_point"),
    col("stats.reb").alias("is_rebound"),
    col("stats.stl").alias("is_steal"),
    col("stats.blk").alias("is_block"),
    col("stats.to").alias("is_turnover"),
    col("stats.pf").alias("is_foul"),
    col("stats.fga").alias("is_attempt"),
    col("stats.fgm").alias("is_made"),
    col("stats.ast").alias("is_assist") 
).filter(col("player_name").isNotNull()) 

# --- AGGREGAZIONE PRINCIPALE ---

# grazie a groupBy("player_name") vado a "unire i dati"
Statistiche_giocatore = data_ready.groupBy("game_id", "player_name", "team_code") \
    .agg(
        sum("is_point").alias("Punti"),
        sum("is_assist").alias("Assist"),
        sum("is_rebound").alias("Rimb"),
        sum("is_steal").alias("Palla_r"),
        sum("is_block").alias("Stopp"), 
        sum("is_turnover").alias("Palla_p"), 
        sum("is_foul").alias("Falli"),       
        sum("is_attempt").alias("FGA"),       
        sum("is_made").alias("FGM"),
        collect_list(when(col("shot_X").isNotNull(), struct(col("shot_X").alias("x"), col("shot_Y").alias("y"), col("is_made").alias("r")))).alias("Posizioni"),
        max(struct("event_ID", "quarter", "clock")).alias("Info_recenti")
    ) \
    .withColumn("%_Tiro", when(col("FGA") > 0, round((col("FGM") / col("FGA")) * 100, 1)).otherwise(0.0)) \
    .withColumnRenamed("game_id", "Partita") \
    .withColumnRenamed("player_name", "Giocatore") \
    .withColumnRenamed("team_code", "Squadra") \
    .orderBy("Partita", "Squadra")
    
    
def process_batch(batch_df, batch_id):
    
    batch_df.persist()  
    
    Info_recenti_Giocatore = batch_df \
        .withColumn("Player_Event_ID", col("Info_recenti.Event_ID"))   \
        .withColumn("Quarto_Giocatore", col("Info_recenti.Quarter")) \
        .withColumn("Clock_Giocatore", col("Info_recenti.Clock"))
        
    # Prendo il Quarto più alto e il clock più basso
    window = Window.partitionBy("Partita").orderBy(col("Quarto_Giocatore").desc(), col("Clock_Giocatore").asc())
    Info_i = Info_recenti_Giocatore.withColumn("i", row_number().over(window))
    Info_recenti_partita = Info_i.filter(col("i") == 1) \
        .select(
            col("Partita"),
            col("Quarto_Giocatore").alias("Quarto_globale"),
            col("Clock_Giocatore").alias("Clock_globale")
        ).drop("Quarto_Giocatore", "Clock_Giocatore")

    # Calcolo il punteggio della Squadra
    Punteggio_Squadra = Info_recenti_Giocatore.groupBy("Partita", "Squadra") \
                            .agg(sum("Punti").alias("Punteggio_Squadra"))
    
    # Calcolo il punteggio della Squadra avversaria
    Punteggio_Squadra_avversaria = Punteggio_Squadra.select(
        col("Partita").alias("Partita_Opp"),
        col("Squadra").alias("Squadra_Opp"),
        col("Punteggio_Squadra").alias("Punteggio_Avversario")
    )
    Punteggio_Squadre = Punteggio_Squadra.join(Punteggio_Squadra_avversaria,
        (col("Partita") == col("Partita_Opp")) & (col("Squadra") != col("Squadra_Opp")), how="left") \
        .drop("Partita_Opp", "Squadra_Opp")
    
    # Unisco tutti i dati
    dati_completi = Info_recenti_Giocatore.join(Punteggio_Squadre, on=["Partita", "Squadra"], how="left") \
        .join(Info_recenti_partita, on=["Partita"], how="left") \
        .drop("Info_recenti", "Player_Event_ID")

    # Prima di inviare mi calcolo: Seconds_Remaining, Score_Diff e Efficiency
    dati_completi = dati_completi \
        .withColumn("Minuti", split(col("Clock_globale"), ":").getItem(0).cast("int")) \
        .withColumn("Secondi", split(col("Clock_globale"), ":").getItem(1).cast("int")) \
        .withColumn("Seconds_Remaining", ((4 - col("Quarto_globale")) * 720) + (col("Minuti") * 60 + col("Secondi"))) \
        .withColumn("Score_Diff", col("Punteggio_Squadra") - col("Punteggio_Avversario")) \
        .withColumn("Efficiency", round(col("Punti") + col("Rimb") + col("Assist") + col("Palla_r") + col("Stopp") - col("Palla_p") - col("Falli") + (col("%_Tiro") / 5), 2)) \
            .drop("Punteggio_Avversario", "Minuti", "Secondi")
        
    # Output JSON
    kafka_output = dati_completi.select(
        col("Partita").alias("key"),
        to_json(struct(
            col("Partita"), 
            col("Squadra"), 
            col("Punteggio_Squadra"), 
            col("Score_Diff"), 
            col("Seconds_Remaining"), 
            col("Quarto_globale"), 
            col("Clock_globale"),
            col("Giocatore"),
            col("Punti"), 
            col("Assist"), 
            col("Rimb"), 
            col("Palla_r"), 
            col("Stopp"),
            col("Palla_p"), 
            col("Falli"), 
            col("%_Tiro"), 
            col("Efficiency"),
            col("Posizioni")
        )).alias("value")
    )
    
    print("Aggrego e invio a kafka...", flush=True)
    kafka_output.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", OUTPUT_TOPIC_ML) \
        .save()

    batch_df.unpersist() 

query = Statistiche_giocatore.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_batch) \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()