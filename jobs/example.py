# 1. Ako izvor ima nove kolone, dodaju se i u tabelu
df = evolve_schema(spark, df, table)

# 2. Poslovne kolone: sve osim kljuca i tehnickih kolona (pocinju sa "_")
biz_cols = [c for c in df.columns if c not in keys and not c.startswith("_")]

# 3. Otisak (hash) reda: menja se cim se promeni bilo koja poslovna vrednost
row_hash = F.xxhash64(*[F.col(c) for c in biz_cols])

# 4. Otisci redova koji se vec nalaze u tabeli
current = spark.table(table).select(*keys, row_hash.alias("old_hash"))

# 5. Zadrzavaju se samo novi redovi (nema ih u tabeli)
#    i izmenjeni redovi (otisak se razlikuje)
changes = (df.withColumn("new_hash", row_hash)
             .join(current, keys, "left")
             .where(F.col("old_hash").isNull()
                    | (F.col("old_hash") != F.col("new_hash")))
             .drop("old_hash", "new_hash")
             .localCheckpoint())   # MERGE trazi izvor koji se ne menja

# 6. Ako nema promena, tabela ostaje ista i ne nastaje novi snimak
if changes.count() == 0:
    return None

# 7. Postojeci redovi se azuriraju, a novi dodaju, u jednoj transakciji
changes.createOrReplaceTempView("changes")
on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
spark.sql(f"""
    MERGE INTO {table} t USING changes s ON {on}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")