# Databricks notebook source
# MAGIC %md
# MAGIC _Masterthesis UvA - Janick Schutz - 2024_
# MAGIC # Regression Model: Interaction Spatial
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Install
install.packages("broom")
install.packages("ggplot2")

# COMMAND ----------

# DBTITLE 1,Imports
library(lfe)
library(ggplot2)
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
  ~ log(wCreditScore) * I(DistrictName)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear | 0 | DistrictCode,
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
  ~ log1p(wOccupancyShare) * I(DistrictName)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear | 0 | DistrictCode,
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
  ~ log1p(wRemainingLeaseTerm) * I(DistrictName)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear | 0 | DistrictCode,
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
  ~ log(wCreditScore) * I(DistrictName)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear | 0 | DistrictCode,
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
  ~ log1p(wOccupancyShare) * I(DistrictName)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear | 0 | DistrictCode,
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
  ~ log1p(wRemainingLeaseTerm) * I(DistrictName)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear | 0 | DistrictCode,
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
  ~ log(wCreditScore) * I(DistrictName)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear | 0 | DistrictCode,  # or ProertyFE?
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
  ~ log1p(wOccupancyShare) * I(DistrictName)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear | 0 | DistrictCode,  # or ProertyFE?
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
  ~ log1p(wRemainingLeaseTerm) * I(DistrictName)
  # controls
  + log(FloorArea)
  + EffectiveAge
  + I(EnergyLabel)
  # regression
  | ValuationYear | 0 | DistrictCode,  # or ProertyFE?
  data = as.data.frame(df3)
)

df_out3c <- tidy(mod3c, conf.int=TRUE)
# summary(mod3c)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plot Coefficients

# COMMAND ----------

# DBTITLE 1,Reusable Plot Method
options(repr.plot.width=1200, repr.plot.height=600)

plot_coef <- function(df){
    ggplot(data=df, aes(x=var, y=estimate, colour=var)) +
        theme_bw() +
        geom_hline(yintercept=0, linetype="solid", color="darkgrey", size=0.5) +
        facet_grid(col=vars(district), scales="free", space="free", switch="x") +
        geom_crossbar(aes(ymin=conf.low, ymax=conf.high), width = 0.4) +
        guides(fill = "none") +
        theme(axis.title.x=element_blank(), axis.text.x=element_blank(), axis.ticks.x=element_blank())
}

# COMMAND ----------

# DBTITLE 1,Plot Office Districts
# prepare data
df1 <- df_out1a[grepl(':I', df_out1a$term), ]
df1$var <- 'wCreditScore'
df1$district <- gsub('log\\(wCreditScore\\):I\\(DistrictName\\)', '', df1$term)

df2 <- df_out1b[grepl(':I', df_out1b$term), ]
df2$var <- 'wOccupancyShare'
df2$district <- gsub('log1p\\(wOccupancyShare\\):I\\(DistrictName\\)', '', df2$term)

df3 <- df_out1c[grepl(':I', df_out1c$term), ]
df3$var <- 'wRemainingLeaseTerm'
df3$district <- gsub('log1p\\(wRemainingLeaseTerm\\):I\\(DistrictName\\)', '', df3$term)

df_plot <- bind_rows(df1, df2, df3)
df_plot <- df_plot <- subset(df_plot, district %in% c('Zuidas', 'Amstel III/Bullewijk', 'Sloterdijk', 'Nieuwmarkt/Lastage', 'Willemspark'))  # subset

# plot
# display(df_plot)
plot_coef(df_plot)

# COMMAND ----------

# DBTITLE 1,Plot Industrial Districts
# prepare data
df1 <- df_out2a[grepl(':I', df_out2a$term), ]
df1$var <- 'wCreditScore'
df1$district <- gsub('log\\(wCreditScore\\):I\\(DistrictName\\)', '', df1$term)

df2 <- df_out2b[grepl(':I', df_out2b$term), ]
df2$var <- 'wOccupancyShare'
df2$district <- gsub('log1p\\(wOccupancyShare\\):I\\(DistrictName\\)', '', df2$term)

df3 <- df_out2c[grepl(':I', df_out2c$term), ]
df3$var <- 'wRemainingLeaseTerm'
df3$district <- gsub('log1p\\(wRemainingLeaseTerm\\):I\\(DistrictName\\)', '', df3$term)

df_plot <- bind_rows(df1, df2, df3)
df_plot <- df_plot <- subset(df_plot, district %in% c('Nieuwe Pijp', 'Oude Pijp', 'Sloterdijk-West', 'Westlandgracht', 'Weesperzijde'))  # subset

# plot
# display(df_plot)
plot_coef(df_plot)

# COMMAND ----------

# DBTITLE 1,Plot Retail Districts
# prepare data
df1 <- df_out3a[grepl(':I', df_out3a$term), ]
df1$var <- 'wCreditScore'
df1$district <- gsub('log\\(wCreditScore\\):I\\(DistrictName\\)', '', df1$term)

df2 <- df_out3b[grepl(':I', df_out3b$term), ]
df2$var <- 'wOccupancyShare'
df2$district <- gsub('log1p\\(wOccupancyShare\\):I\\(DistrictName\\)', '', df2$term)

df3 <- df_out3c[grepl(':I', df_out3c$term), ]
df3$var <- 'wRemainingLeaseTerm'
df3$district <- gsub('log1p\\(wRemainingLeaseTerm\\):I\\(DistrictName\\)', '', df3$term)

df_plot <- bind_rows(df1, df2, df3)
df_plot <- subset(df_plot, district %in% c('Burgwallen-Nieuwe Zijde', 'Burgwallen-Oude Zijde', 'Grachtengordel-West', 'Nieuwe Pijp', 'De Weteringschans'))  # subset

# plot
# display(df_plot)
plot_coef(df_plot)

# COMMAND ----------


