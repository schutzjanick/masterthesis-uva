# Databricks notebook source
# MAGIC %md
# MAGIC _Masterthesis UvA - Janick Schutz - 2024_
# MAGIC # Regression Model: Full Market Effects
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Imports
library(lfe)
library(SparkR)
library(stargazer)

sparkR.session()

# COMMAND ----------

# overall market dataset
source_path_overall = "/mnt/da-lab/users/janick_schutz/thesis/1_data_preparation/final"
df_overall = read.df(source_path_overall)
rdf_overall = as.data.frame(df_overall)
rdf_overall$CapFactor <- rdf_overall$CapFactorTRI   # CapFactorTRI / CapFactorERV

# repeated-sales dataset
source_path_repeat = "/mnt/da-lab/users/janick_schutz/thesis/1_data_preparation/final_repeat"
df_repeat = read.df(source_path_repeat)
rdf_repeat = as.data.frame(df_repeat)
rdf_repeat$CapFactor <- rdf_repeat$CapFactorTRI  # CapFactorTRI / CapFactorERV

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regression Models

# COMMAND ----------

# DBTITLE 1,wOccupancyShare
df1 <- rdf_overall

mod1 <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log1p(wOccupancyShare)
  # controls
  + log1p(FloorArea)
  + I(PropertyType)
  + EffectiveAge
  + I(EnergyLabel)
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df1)
)

summary(mod1)

# COMMAND ----------

# MAGIC %md
# MAGIC Diviersification would suggest a negative coefficient for AvgOccupancyShare

# COMMAND ----------

# DBTITLE 1,wRemainingLeaseTerm
df2 <- rdf_overall

mod2 <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log1p(wRemainingLeaseTerm)
  # controls
  + log1p(FloorArea)
  + I(PropertyType)
  + EffectiveAge
  + I(EnergyLabel)
  # other
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df2)
)

summary(mod2)

# COMMAND ----------

# DBTITLE 1,wOccupancyShare & wRemainingLeaseTerm
df3 <- rdf_overall

mod3 <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log1p(wOccupancyShare)
  + log1p(wRemainingLeaseTerm)
  # controls
  + log(FloorArea)
  + I(PropertyType)
  + EffectiveAge
  + I(EnergyLabel)
  # other
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df3)
)

summary(mod3)

# COMMAND ----------

# DBTITLE 1,wOccupancyShare & wRemainingLeaseTerm
df4 <- rdf_repeat

mod4 <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log1p(wOccupancyShare)
  + log1p(wRemainingLeaseTerm)
  # other
  | ValuationYear + CadastrePropertyID | 0 | DistrictCode,
  data = as.data.frame(df4)
)

summary(mod4)

# COMMAND ----------

# MAGIC %md
# MAGIC Cluster the standart errors based on PropertyType instead of MunicipalityCode leads to similar results, however, some significe levels are somewhat lower.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Summary

# COMMAND ----------

star_out <- stargazer(
  mod1,
  mod2,
  mod3,
  mod4,
  add.lines = list(
    c("EnergyLabels", "Yes", "Yes", "Yes", "Yes"),
    c("Fixed-Effects Year", "Yes", "Yes", "Yes", "Yes"),
    c("Fixed-Effects District", "Yes", "Yes", "Yes", "No"),
    c("Fixed-Effects Property", "No", "No", "No", "Yes")
  ),
  omit = c("PropertyType", "EnergyLabel", "Term)unknown"),
  title = "Fixed Effects Regression - Complete Model",
  font.size = "footnotesize",
  no.space = TRUE,
  flip = TRUE,
  column.sep.width = "0.5pt",
  single.row = FALSE,
  type="text"
)

# COMMAND ----------


