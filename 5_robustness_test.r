# Databricks notebook source
# MAGIC %md
# MAGIC _Masterthesis UvA - Janick Schutz - 2024_
# MAGIC # Regression Model: Robustness Tests Overvall Model
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Install
install.packages("broom")

# COMMAND ----------

# DBTITLE 1,Imports
library(lfe)
library(dplyr)
library(broom)
library(stargazer)
library(SparkR)

sparkR.session()

# COMMAND ----------

# DBTITLE 1,Extract Prepared Data
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
# MAGIC Robustness Regression - COMPLET DATASET 

# COMMAND ----------

# MAGIC %md
# MAGIC Same as Model (4) but then with CapFactor(ERV)

# COMMAND ----------

df1 <- rdf_overall #MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437'))
#modRobERV
modr1 <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log1p(wCreditScore)
  + log1p(wOccupancyShare)
  + log1p(wRemainingLeaseTerm)
  # controls
  + log(FloorArea)
  + I(PropertyType)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df1)
)

summary(modr1)

# COMMAND ----------

# MAGIC %md
# MAGIC CatCreditScore

# COMMAND ----------


df2 <- rdf_overall 
df2$CatCreditScore <- factor(df2$CatCreditScore)
df2$CatCreditScore <- relevel(df2$CatCreditScore, ref = "B")

#modRobCatCreditScore
modr2 <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ CatCreditScore
  # controls
  + log(FloorArea)
  + I(PropertyType)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df2)
)


summary(modr2)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC CatOccupancyShare

# COMMAND ----------

df3 <- rdf_overall 
df3$CatOccupancyShare <- factor(df2$CatOccupancyShare)
df3$CatOccupancyShare <- relevel(df2$CatOccupancyShare, ref = "Multi-Tenant")
#modRobCatOccupancyShare
modr3 <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ CatOccupancyShare 
  # controls
  + log(FloorArea)
  + I(PropertyType)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df3)
)


summary(modr3)

# COMMAND ----------

# MAGIC %md
# MAGIC CatRemainingLease

# COMMAND ----------

df4 <- rdf_overall #MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437'))
df4$catRemainingLeaseTerm <- factor(df2$catRemainingLeaseTerm)
df4$catRemainingLeaseTerm <- relevel(df2$catRemainingLeaseTerm, ref = "average")
#modRobCatRemainingLeaseTerm
modr4 <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ catRemainingLeaseTerm
  # controls
  + log(FloorArea)
  + I(PropertyType)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df4)
)


summary(modr4)

# COMMAND ----------

# MAGIC %md
# MAGIC Proxi Credit Score

# COMMAND ----------

df5 <- rdf_overall
#df5$CatOrganizationAge <- factor(df2$CatOrganizationAge)
#df5$CatOrganizationAge <- relevel(df2$CatOrganizationAge, ref = "medium")
#modRobProxyCreditScoreOA
modr5 <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ CatOrganizationAge
  # controls
  + log(FloorArea)
  + I(PropertyType)
  + EffectiveAge
  + I(EnergyLabel)
  # other
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df5)
)

summary(modr5)

# COMMAND ----------

df6 <- rdf_overall
#df6$CatLaborForce <- relevel(df2$CatLaborForce, ref = "medium")
#modRobProxyCreditScoreLF
modr6 <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ CatLaborForce
  # controls
  + log(FloorArea)
  + I(PropertyType)
  + EffectiveAge
  + I(EnergyLabel)
  # other
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df6)
)

summary(modr6)

# COMMAND ----------

# MAGIC %md
# MAGIC ProxyRemaingLeaseTerm
# MAGIC

# COMMAND ----------

df7 <- rdf_overall
#modRobProxyRemainingLeaseTermExcl
modr7 <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log1p(wRemainingLeaseTerm2)
  # controls
  + log(FloorArea)
  + I(PropertyType)
  + EffectiveAge
  + I(EnergyLabel)
  # other
  | ValuationYear + DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df7)
)

summary(modr7)

# COMMAND ----------

# MAGIC %md
# MAGIC Clustered Standart Erros on PropertyID

# COMMAND ----------

df8 <- rdf_overall
#modRobClusterStdErrorPropertyID
modr8 <- felm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log1p(wCreditScore)
  + log1p(wOccupancyShare)
  + log1p(wRemainingLeaseTerm)
  # controls
  + log(FloorArea)
  + I(PropertyType)
  + EffectiveAge
  + I(EnergyLabel)
  # other
  | ValuationYear + DistrictCode | 0 | CadastrePropertyID,
  data = as.data.frame(df8)
)

summary(modRobClusterStdErrorPropertyID)

# COMMAND ----------

# MAGIC %md
# MAGIC R^2 Omitting wCreditScore

# COMMAND ----------

df9 <- rdf_overall
#modRsquaredOmittedCS
modr9 <- felm(
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
  data = as.data.frame(df9)
)

summary(modr9)

# COMMAND ----------

# MAGIC %md
# MAGIC Moved from other notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC **Robustnes: How does R^2 and adj. R^2 change if we leave wCreditRating out of Model 4 and Model 5Rep?

# COMMAND ----------

star_out <- stargazer(
#(modr1), 
(modr2), (modr4), (modr5),  (modr6), 
#(modr3),
#(modr7), 
#(modr8), 
#(modr9),
  add.lines = list(
    c("EnergyLabels", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("Fixed effect Year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("Fixed effect Location", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("Fixed effect Property", "No", "No", "No", "No", "No", "No", "No")
  ),
  omit = c("^PropertySubType$", "^Energylabel$"),
  title = "Rebustness Test",
  font.size = "footnotesize",
  no.space = TRUE,
  flip = TRUE,
  column.sep.width = "0.5pt",
  single.row = FALSE,
  type="text"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
