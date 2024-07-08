# Databricks notebook source
# MAGIC %md
# MAGIC _Masterthesis UvA - Janick Schutz - 2024_
# MAGIC # Regression Model: Breakdown Real Estate Segment Effects
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Imports
library(lfe)
library(SparkR)
library(stargazer)

sparkR.session()

# COMMAND ----------

# DBTITLE 1,Extract Prepared Data
# overall market dataset
source_path_overall = "/mnt/da-lab/users/janick_schutz/thesis/1_data_preparation/final"
df_overall = read.df(source_path_overall)
rdf_overall = as.data.frame(df_overall)
rdf_overall <- subset(rdf_overall, wCreditScore > 0 & wRemainingLeaseTerm > 0 & MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437'))
rdf_overall$CapFactor <- rdf_overall$CapFactorTRI   # CapFactorTRI / CapFactorERV

# repeated-sales dataset
source_path_repeat = "/mnt/da-lab/users/janick_schutz/thesis/1_data_preparation/final_repeat"
df_repeat = read.df(source_path_repeat)
rdf_repeat = as.data.frame(df_repeat)
rdf_repeat <- subset(rdf_repeat, wCreditScore > 0 & wRemainingLeaseTerm > 0 & MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437'))
rdf_repeat$CapFactor <- rdf_repeat$CapFactorTRI   # CapFactorTRI / CapFactorERV

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segment Office

# COMMAND ----------

# DBTITLE 1,Regression 1a: Office District FE
df1a <- subset(rdf_overall, PropertyType == "Office")

mod1a <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log(wCreditScore)
  + log1p(wOccupancyShare)
  + log1p(wRemainingLeaseTerm)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(rdf_overall)
)

summary(mod1a)

# COMMAND ----------

df1b <- subset(rdf_overall, PropertyType == "Office")

mod1b <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log(wCreditScore) * catRemainingLeaseTerm
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(rdf_overall)
)

summary(mod1b)

# COMMAND ----------

# DBTITLE 1,Regression 1b: Office Property FE
df1c <- subset(rdf_repeat, PropertyType == "Office")

mod1c <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log(wCreditScore) * catRemainingLeaseTerm
  # regression
  | ValuationYear + CadastrePropertyID | 0 | DistrictCode,
  data = as.data.frame(df1c)
)

summary(mod1c)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segment Industrial

# COMMAND ----------

# DBTITLE 1,Regression 2a: Industrial District FE
df2a <- subset(rdf_overall, PropertyType == "Industrial")

mod2a <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log(wCreditScore)
  + log1p(wOccupancyShare)
  + log1p(wRemainingLeaseTerm)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df2a)
)

summary(mod2a)

# COMMAND ----------

df2b <- subset(rdf_overall, PropertyType == "Industrial")

mod2b <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log(wCreditScore) * catRemainingLeaseTerm
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df2b)
)

summary(mod2a)

# COMMAND ----------

# DBTITLE 1,Regression 2b: Industrial Property FE
df2c <- subset(rdf_repeat, PropertyType == "Industrial")

mod2c <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log(wCreditScore) * catRemainingLeaseTerm
  # other
  | ValuationYear + CadastrePropertyID | 0 | DistrictCode,
  data = as.data.frame(df2c)
)

summary(mod2b)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segment Retail

# COMMAND ----------

# DBTITLE 1,Regression 3a: Retail District FE
df3a <- subset(rdf_overall, PropertyType == "Retail")

mod3a <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log(wCreditScore)
  + log1p(wOccupancyShare)
  + log1p(wRemainingLeaseTerm)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # + EnergyLabel
  # other
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df3a)
)

summary(mod3a)

# COMMAND ----------

df3b <- subset(rdf_overall, PropertyType == "Retail")

mod3b <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log(wCreditScore) * catRemainingLeaseTerm
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # + EnergyLabel
  # other
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df3b)
)

summary(mod3b)

# COMMAND ----------

# DBTITLE 1,Regression 3a: Retail Property FE
df3c <- subset(rdf_repeat, PropertyType == "Retail")

mod3c <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log(wCreditScore) * catRemainingLeaseTerm
  # other
  | ValuationYear + CadastrePropertyID | 0 | DistrictCode,
  data = as.data.frame(df3c)
)

summary(mod3c)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Summary

# COMMAND ----------

# DBTITLE 1,Output Text
star_out <- stargazer(
  # office
  # mod1a,
  mod1b,
  mod1c,
  # industrial
  # mod2a,
  mod2b,
  mod2c,
  # retail
  # mod3a,
  mod3b,
  mod3c,
  # settings
  add.lines = list(
    c("EnergyLabels", "Yes", "No", "Yes", "No", "Yes", "No"),
    c("Fixed-Effects Year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("Fixed-Effects District", "Yes", "No", "Yes", "No", "Yes", "No"),
    c("Fixed-Effects Property", "No", "Yes", "No", "Yes", "No", "Yes")
  ),
  omit = c("EnergyLabel"),
  title = "Fixed Effects Regression - Breakdown Per Segment",
  font.size = "footnotesize",
  no.space = TRUE,
  flip = TRUE,
  column.sep.width = "0.5pt",
  single.row = FALSE,
  type="text"
)

# COMMAND ----------


