# Databricks notebook source
# MAGIC %md
# MAGIC driver standings, driver, races, seasons, circuits:
# MAGIC - cual es el piloto con más carreras corridas de la historia? 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT drv.forename, drv.surname, driveId_max_count
# MAGIC FROM gold.DimDrivers AS drv
# MAGIC INNER JOIN (
# MAGIC     SELECT driverId, COUNT(driverId) AS driveId_max_count
# MAGIC     FROM gold.FactDriver_Standings
# MAGIC     GROUP BY driverId
# MAGIC     ORDER BY COUNT(driverId) DESC
# MAGIC     LIMIT 1
# MAGIC ) AS races_count
# MAGIC     ON races_count.driverId = drv.driverId

# COMMAND ----------

# MAGIC %md
# MAGIC - cual es piloto con más carreras ganadas de la historia?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT drv.forename, drv.surname, wins_count.driveId_win_count
# MAGIC FROM gold.DimDrivers AS drv
# MAGIC INNER JOIN (
# MAGIC     SELECT driverId, COUNT(driverId) AS driveId_win_count
# MAGIC     FROM gold.FactDriver_Standings
# MAGIC     WHERE wins = 1
# MAGIC     GROUP BY driverId
# MAGIC     ORDER BY COUNT(driverId) DESC
# MAGIC     LIMIT 1
# MAGIC ) AS wins_count
# MAGIC     ON wins_count.driverId = drv.driverId
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC constructor_standings, constructors 
# MAGIC - cual es la escudería más ganadora? 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT constr.name, wins_count.constructorId_win_count
# MAGIC FROM gold.DimConstructors AS constr
# MAGIC INNER JOIN (
# MAGIC     SELECT constructorId, COUNT(constructorId) AS constructorId_win_count
# MAGIC     FROM gold.FactConstructor_Standings
# MAGIC     WHERE wins = 1
# MAGIC     GROUP BY constructorId
# MAGIC     ORDER BY COUNT(constructorId) DESC
# MAGIC     LIMIT 1
# MAGIC ) AS wins_count
# MAGIC     ON wins_count.constructorId = constr.constructorId

# COMMAND ----------

# MAGIC %md
# MAGIC - cual es el circuito más corrido?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH most_played_circuit AS (
# MAGIC   SELECT
# MAGIC       circuitId, COUNT(circuitId) AS circuitId_plays_count
# MAGIC FROM gold.DimRaces
# MAGIC GROUP BY circuitId
# MAGIC ORDER BY COUNT(circuitId) DESC
# MAGIC LIMIT 1
# MAGIC ), most_played_circuit_name AS (
# MAGIC   SELECT Dimcircuits.circuitId, Dimcircuits.name, most_played_circuit.circuitId_plays_count
# MAGIC FROM most_played_circuit
# MAGIC INNER JOIN gold.Dimcircuits 
# MAGIC   ON Dimcircuits.circuitId = most_played_circuit.circuitId
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM most_played_circuit_name
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - cual es el piloto más rápido del circuito más corrido?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH races_circuits AS
# MAGIC (
# MAGIC     SELECT raceId, circuitId
# MAGIC     FROM gold.DimRaces
# MAGIC
# MAGIC ), most_played AS
# MAGIC (
# MAGIC     SELECT
# MAGIC           circuitId, COUNT(circuitId) AS circuitId_plays_count
# MAGIC     FROM gold.DimRaces
# MAGIC     GROUP BY circuitId
# MAGIC     ORDER BY COUNT(circuitId) DESC
# MAGIC     LIMIT 1
# MAGIC   ), raceids_mostplayed AS (
# MAGIC     SELECT raceId,races_circuits.circuitId
# MAGIC     FROM most_played
# MAGIC     INNER JOIN races_circuits
# MAGIC       ON races_circuits.circuitId = most_played.circuitId
# MAGIC   )
# MAGIC   , races_milliseconds AS
# MAGIC   (
# MAGIC       SELECT raceId, milliseconds
# MAGIC       FROM gold.FactResults
# MAGIC       WHERE position = 1
# MAGIC       GROUP BY raceId, milliseconds
# MAGIC   ), fastest_driver(
# MAGIC   SELECT res.raceId, res.driverId, MIN(res.milliseconds) as min_milliseconds
# MAGIC   FROM gold.FactResults res
# MAGIC   INNER JOIN raceids_mostplayed
# MAGIC     ON raceids_mostplayed.raceId = res.raceId
# MAGIC   GROUP BY res.raceId, res.driverId
# MAGIC   ORDER BY MIN(res.milliseconds) DESC
# MAGIC   LIMIT 1     
# MAGIC   )
# MAGIC   SELECT fastest_driver.raceId, drv.DriverId, drv.forename, drv.surname, fastest_driver.min_milliseconds
# MAGIC   FROM fastest_driver
# MAGIC   INNER JOIN gold.DimDrivers drv
# MAGIC     ON drv.driverId = fastest_driver.driverId
