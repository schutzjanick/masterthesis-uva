# Databricks notebook source
# MAGIC %md
# MAGIC _Masterthesis UvA - Janick Schutz - 2024_
# MAGIC # Regression Model: Interaction Temporal
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Install
install.packages("broom")
install.packages("ggplot2")

# COMMAND ----------

# DBTITLE 1,Imports
library(lfe)
library(dplyr)
library(ggplot2)
library(broom)
library(stringr)
library(stargazer)
library(SparkR)
require(tidyverse)
library(tidyr)

sparkR.session()

# COMMAND ----------

# DBTITLE 1,Extract Prepared Data
# overall market dataset
source_path_overall = "/mnt/da-lab/users/janick_schutz/thesis/1_data_preparation/final"
df_overall = read.df(source_path_overall)
rdf_overall = as.data.frame(df_overall)
rdf_overall$CapFactor <- rdf_overall$CapFactorTRI   # CapFactorTRI / CapFactorERV

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segment Office

# COMMAND ----------

# DBTITLE 1,Regression 1a: Office wCreditScore
df1 <- subset(rdf_overall, PropertyType == "Office" & MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437'))
df1 <- subset(df1, ValuationYear > 2016 & ValuationYear < 2024)

mod1a <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log(wCreditScore) * factor(ValuationYear)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  |  DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df1)
)

df_out1a <- tidy(mod1a, conf.int=TRUE)
# summary(mod1a)

# COMMAND ----------

# DBTITLE 1,Regression 1b: Office wOccupancyShare
df1 <- subset(rdf_overall, PropertyType == "Office")  # & MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437')
df1 <- subset(df1, ValuationYear > 2016 & ValuationYear < 2024)

mod1b <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log1p(wOccupancyShare) * factor(ValuationYear)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df1)
)

df_out1b <- tidy(mod1b, conf.int=TRUE)
# summary(mod1b)

# COMMAND ----------

# DBTITLE 1,Regression 1c: Office wRemainingLeaseTerm
df1 <- subset(rdf_overall, PropertyType == "Office")  # & MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437')
df1 <- subset(df1, ValuationYear > 2016 & ValuationYear < 2024)

mod1c <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log1p(wRemainingLeaseTerm) * factor(ValuationYear)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | DistrictCode | 0 | DistrictCode,
  data = as.data.frame(df1)
)

df_out1c <- tidy(mod1c, conf.int=TRUE)
# summary(mod1c)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segment Industrial

# COMMAND ----------

# DBTITLE 1,Regression 2a: Industrial wCreditScore
df2 <- subset(rdf_overall, PropertyType == "Industrial" & MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437'))
df2 <- subset(df2, ValuationYear > 2016 & ValuationYear < 2024)

mod2a <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log(wCreditScore) * factor(ValuationYear)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | DistrictName | 0 | DistrictCode,
  data = as.data.frame(df2)
)

df_out2a <- tidy(mod2a, conf.int=TRUE)
# summary(mod2a)

# COMMAND ----------

# DBTITLE 1,Regression 1b: Office wOccupancyShare
df2 <- subset(rdf_overall, PropertyType == "Industrial")  # & MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437')
df2 <- subset(df2, ValuationYear > 2016 & ValuationYear < 2024)

mod2b <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log1p(wOccupancyShare) * factor(ValuationYear)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | DistrictName | 0 | DistrictCode,
  data = as.data.frame(df2)
)

df_out2b <- tidy(mod2b, conf.int=TRUE)
# summary(mod2b)

# COMMAND ----------

# DBTITLE 1,Regression 1c: Industrial wRemainingLeaseTerm
df2 <- subset(rdf_overall, PropertyType == "Industrial")  # & MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437')
df2 <- subset(df2, ValuationYear > 2016 & ValuationYear < 2024)

mod2c <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log1p(wRemainingLeaseTerm) * factor(ValuationYear)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | DistrictName | 0 | DistrictCode,
  data = as.data.frame(df2)
)

df_out2c <- tidy(mod2c, conf.int=TRUE)
# summary(mod2c)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segment Retail

# COMMAND ----------

# DBTITLE 1,Regression 3a: Retail wCreditScore
df3 <- subset(rdf_overall, PropertyType == "Retail" & MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437'))
df3 <- subset(df3, ValuationYear > 2016 & ValuationYear < 2024)

mod3a <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log(wCreditScore) * factor(ValuationYear)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | DistrictName | 0 | DistrictCode,
  data = as.data.frame(df3)
)

df_out3a <- tidy(mod3a, conf.int=TRUE)
# summary(mod3a)

# COMMAND ----------

# DBTITLE 1,Regression 1b: Office wOccupancyShare
df3 <- subset(rdf_overall, PropertyType == "Retail")  # & MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437')
df3 <- subset(df3, ValuationYear > 2016 & ValuationYear < 2024)

mod3b <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log1p(wOccupancyShare) * factor(ValuationYear)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | DistrictName | 0 | DistrictCode,
  data = as.data.frame(df3)
)

df_out3b <- tidy(mod3b, conf.int=TRUE)
# summary(mod3b)

# COMMAND ----------

# DBTITLE 1,Regression 1c: Retail  wRemainingLeaseTerm
df3 <- subset(rdf_overall, PropertyType == "Retail")  # & MunicipalityCode %in% c('0362', '0363', '0394', '0384', '0437')
df3 <- subset(df3, ValuationYear > 2016 & ValuationYear < 2024)

mod3c <- felm(
  # dependent
  log(CapFactor)
  # variables of interest
  ~ log1p(wRemainingLeaseTerm) * factor(ValuationYear)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | DistrictName | 0 | DistrictCode,
  data = as.data.frame(df3)
)

df_out3c <- tidy(mod3c, conf.int=TRUE)
# summary(mod3c)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plot Coefficients
# MAGIC

# COMMAND ----------

# DBTITLE 1,Collect Data
# office
df_plot1a <- df_out1a[grepl(':factor', df_out1a$term), ]
df_plot1a$var <- 'wCreditScore'
df_plot1b <- df_out1b[grepl(':factor', df_out1b$term), ]
df_plot1b$var <- 'wOccupancyShare'
df_plot1c <- df_out1c[grepl(':factor', df_out1c$term), ]
df_plot1c$var <- 'wRemainingLeaseTerm'

df_plot1 <- bind_rows(df_plot1a, df_plot1b, df_plot1c)
df_plot1$segment <- 'Office'
df_plot1$year <- as.numeric(str_sub(df_plot1$term, -4, -1))

# industrial
df_plot2a <- df_out2a[grepl(':factor', df_out2a$term), ]
df_plot2a$var <- 'wCreditScore'
df_plot2b <- df_out2b[grepl(':factor', df_out2b$term), ]
df_plot2b$var <- 'wOccupancyShare'
df_plot2c <- df_out2c[grepl(':factor', df_out2c$term), ]
df_plot2c$var <- 'wRemainingLeaseTerm'

df_plot2 <- bind_rows(df_plot2a, df_plot2b, df_plot2c)
df_plot2$segment <- 'Industrial'
df_plot2$year <- as.numeric(str_sub(df_plot2$term, -4, -1))

# retail
df_plot3a <- df_out3a[grepl(':factor', df_out3a$term), ]
df_plot3a$var <- 'wCreditScore'
df_plot3b <- df_out3b[grepl(':factor', df_out3b$term), ]
df_plot3b$var <- 'wOccupancyShare'
df_plot3c <- df_out3c[grepl(':factor', df_out3c$term), ]
df_plot3c$var <- 'wRemainingLeaseTerm'

df_plot3 <- bind_rows(df_plot3a, df_plot3b, df_plot3c)
df_plot3$segment <- 'Retail'
df_plot3$year <- as.numeric(str_sub(df_plot3$term, -4, -1))

# overall
df_plot <- bind_rows(df_plot1, df_plot2, df_plot3)
df_plot$Segment = factor(df_plot$segment, levels=c('Office','Industrial','Retail'))

# COMMAND ----------

# DBTITLE 1,Plot Results
options(repr.plot.width=1200, repr.plot.height=600)

ggplot(data=df_plot, aes(x=year, y=estimate, colour=var)) +
  theme_bw() +
  geom_point() +
  geom_line() +
  facet_grid(cols=vars(Segment), scales="free", space="free", switch="y") +
  theme(legend.position="bottom") +
  geom_hline(yintercept=0, linetype="solid", color="darkgrey", size=0.5) +
  geom_ribbon(aes(ymin=conf.low, ymax=conf.high), linetype="dotted", size=0.4, alpha=0.1)

# COMMAND ----------


