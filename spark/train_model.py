from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml import Pipeline

spark = SparkSession.builder \
    .appName("NBA_Training_Double_Model") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#leggo il dataset
data = spark.read.parquet("/app/pipeline_data/training_dataset_estratto")
data = data.dropDuplicates(['Partita', 'Giocatore', 'Seconds_Remaining'])
data = data.na.fill(0.0)

# --- PREPARAZIONE LABEL ---

#Chi ha vinto?  Obiettivo: Per ogni istante della partita, dobbiamo sapere se quella squadra alla fine ha vinto o perso.
window_partita = Window.partitionBy("Partita","Squadra").orderBy(F.col("Seconds_Remaining").asc())
label_win = data.withColumn("Final_Diff", F.first("Score_Diff").over(window_partita)) \
                            .withColumn("Label_Win", F.when(F.col("Final_Diff") > 0, 1).otherwise(0))

# Normalizzo Efficiency
window_Squadra = Window.partitionBy("Partita", "Squadra","Seconds_Remaining")
label_win = label_win.withColumn("Total_Eff_Moment", F.sum("Efficiency").over(window_Squadra)) \
    .withColumn("Efficiency_Normalized", F.when(F.col("Total_Eff_Moment") > 0, F.round(F.col("Efficiency") / F.col("Total_Eff_Moment"), 4)).otherwise(0.0))

# Chi è l'MVP? Obiettivo: Identificare il giocatore migliore della partita.
window_partita_totale = Window.partitionBy("Partita")
label_full = label_win.withColumn("Max_Game_Eff", F.max("Efficiency").over(window_partita_totale)) \
                     .withColumn("Label_MVP", F.when(F.col("Efficiency") == F.col("Max_Game_Eff"), 1) .otherwise(0))

# --- TRAINING MODELLO WIN ---

print("Allenamento Modello 1: WIN PROBABILITY...")
assembler_win = VectorAssembler(
    inputCols=["Score_Diff", "Seconds_Remaining"], 
    outputCol="features_win"
    )

rf_win = RandomForestClassifier(labelCol="Label_Win", featuresCol="features_win", numTrees=50)
pipeline_win = Pipeline(stages=[assembler_win, rf_win])
model_win = pipeline_win.fit(label_full)
model_win.write().overwrite().save("/tmp/models/win_model")
print("Modello 1 (Win Probability) salvato.")

# --- TRAINING MODELLO MVP ---

print("Allenamento Modello 2: MVP PREDICTOR...")
assembler_mvp = VectorAssembler(
    inputCols=["Punti", "Assist", "Rimb", "Palla_r", "Stopp", "Palla_p", "Falli","%_Tiro", "Efficiency_Normalized"], 
    outputCol="features_mvp"
    )

lr_mvp = LogisticRegression(labelCol="Label_MVP", featuresCol="features_mvp", maxIter=20)
pipeline_mvp = Pipeline(stages=[assembler_mvp, lr_mvp])
model_mvp = pipeline_mvp.fit(label_full)
model_mvp.write().overwrite().save("/tmp/models/mvp_model")
print("Modello 2 (MVP Probability) salvato.")

print("TRAINING COMPLETATO CON SUCCESSO!")
spark.stop()