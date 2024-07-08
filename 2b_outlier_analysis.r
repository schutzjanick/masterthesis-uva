# Databricks notebook source
# MAGIC %md
# MAGIC _Masterthesis UvA - Janick Schutz - 2024_
# MAGIC # Outlier Analysis
# MAGIC ---

# COMMAND ----------

library(ggplot2)
library(dplyr)
library(stargazer)

# plot size
options(repr.plot.width = 800, repr.plot.height = 600) 

# COMMAND ----------

source_path = "/dbfs/mnt/da-lab/users/janick_schutz/thesis/1_data_preparation/final.csv"
df = read.csv(source_path)
rdf = as.data.frame(df)

# COMMAND ----------

# DBTITLE 1,Parameter
rdf$CapFactor <- rdf$CapFactorTRI  # CapFactorERV / CapFactorTRI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discordant values
# MAGIC
# MAGIC We deal with the discordant values.  We take a 'label and test' approach, by labeling the discordant values in this step and then testing for their impact in the modeling step that follows.  
# MAGIC
# MAGIC We start by adding three new fields to the sales data:
# MAGIC
# MAGIC 1. Discordant: Binary indicating discordant or not
# MAGIC 2. disc_fields: Text field with all fields that are discordant
# MAGIC 3. disc_type: Text field with the type of discordancy for each discordant field
# MAGIC
# MAGIC Each of these is set to 0 or blank to begin.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Univariate (UV) discordants
# MAGIC
# MAGIC First, we apply a discordant label to a number of key fields based on their values.  Particularly, we label all values below the 1% quantile or above the 99% quantile as potentially discordant. These are Univariate (UV) discordants.

# COMMAND ----------

# # cap rate (tri)
# spd <- which(rdf$CapFactor > quantile(rdf$CapFactor, .995) |
#              rdf$CapFactor < quantile(rdf$CapFactor, .01))
# spd$discordant_uv1[spd] <- 1
# spd$disc_fields[spd] <- paste0(rdf$disc_fields[spd], 'CapFactor | ')
# spd$disc_type[spd] <- paste0(rdf$disc_type[spd], 'UV: 1% Quant | ')

# # credit score
# spd <- which(rdf$CreditScore < quantile(rdf$CreditScore, .01))
# rdf$discordant_uv1[spd] <- 1
# rdf$disc_fields[spd] <- paste0(rdf$disc_fields[spd], 'CreditScore | ')
# rdf$disc_type[spd] <- paste0(rdf$disc_type[spd], 'UV: 1% Quant | ')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Multivariate (MV) discordants
# MAGIC
# MAGIC Finally, we find the LFA and price relationship to be one of the strongest relationships in the data.  From this relationship we label multivariate (MV) discordant values using a multiple step process. 
# MAGIC ```
# MAGIC # TURNED OFF
# MAGIC # rdf$discordant_mv1[mah.out$x.disc.5] <- 1
# MAGIC # rdf$disc_fields[mah.out$x.disc.5] <- paste0(rdf$disc_fields[mah.out$x.disc.5], 'CapFactor vs wCreditScore | ')
# MAGIC # rdf$disc_type[mah.out$x.disc.5] <- paste0(rdf$disc_type[mah.out$x.disc.5], 'MV: 1% Mah Quant | ')
# MAGIC # rdf$discordant_mv2[mah.out$x.disc.6] <- 1
# MAGIC # rdf$disc_fields[mah.out$x.disc.6] <- paste0(rdf$disc_fields[mah.out$x.disc.6], 'CapFactor vs wCreditScore | ')
# MAGIC # rdf$disc_type[mah.out$x.disc.6] <- paste0(rdf$disc_type[mah.out$x.disc.6], 'MV: 0.1% Mah Quant | ')
# MAGIC ```
# MAGIC
# MAGIC We begin by defining a customized function for the output

# COMMAND ----------

# DBTITLE 1,Reusable Plot Method
#  mahalabonis distance function
mahalabonis.dist <- function(df, y, x, ylabel, xlabel){

  # multivariate discordancy
  x.data <- data.frame(y = df[,deparse(substitute(y))],
                       x = df[,deparse(substitute(x))])
  x.data$x.mah <- mahalanobis(x.data, colMeans(x.data), cov(x.data))
  cuts <- quantile(x.data$x.mah, probs=c(0, .8, .9, .95, .99, .999, 1))
  x.data$mah.col <- as.numeric(as.factor(cut(x.data$x.mah, cuts))) 
  x.data$discordant <- df$discordant

  # outliers
  x.disc.5 <- which(x.data$mah.col > 4)
  x.disc.6 <- which(x.data$mah.col > 5)

  # plot
  plot <- ggplot(x.data, aes(x=x, y=y, color=as.factor(mah.col))) +
    xlab(xlabel) + ylab(ylabel) +
    theme_bw() + geom_point() +
    scale_color_manual(values=c('gray70', 'gray55', 'gray40', 'gray15', 'orange', 'red'))

  # output
  my_list <- list("plot" = plot, "x.disc.5" = x.disc.5, "x.disc.6" = x.disc.6)

  return(my_list)
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Office

# COMMAND ----------

# MAGIC %md
# MAGIC #### Office CreditScore

# COMMAND ----------

# DBTITLE 1,MV Plot wCreditScore - Level
dftmp <- rdf %>% filter(!is.na(wCreditScore)) %>% filter(PropertyType == 'Office')

mah.out <- mahalabonis.dist(dftmp, CapFactor, wCreditScore, "ln(CapFactor)", "wCreditScore")
print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# DBTITLE 1,MV Plot wCreditScore - Log-Log
dftmp <- rdf %>% filter(!is.na(wCreditScore)) %>% filter(PropertyType == 'Office')
dftmp$CapFactor <- log(dftmp$CapFactor)
dftmp$wCreditScore <- log(dftmp$wCreditScore)

mah.out <- mahalabonis.dist(dftmp, CapFactor, wCreditScore, "ln(CapFactor)", "ln(wCreditScore)")
print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# MAGIC %md
# MAGIC #### Office OccupancyShare

# COMMAND ----------

# DBTITLE 1,MV Plot wOccupancyShare - Level
dftmp <- rdf %>% filter(!is.na(wOccupancyShare)) %>% filter(PropertyType == 'Office')
mah.out <- mahalabonis.dist(dftmp, CapFactor, wOccupancyShare, "CapFactor", "wOccupancyShare")

print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# DBTITLE 1,MV Plot wOccupancyShare - Log-Log
dftmp <- rdf %>% filter(!is.na(wCreditScore)) %>% filter(PropertyType == 'Office')
dftmp$CapFactor <- log1p(dftmp$CapFactor)
dftmp$wOccupancyShare <- log1p(dftmp$wOccupancyShare)

mah.out <- mahalabonis.dist(dftmp, CapFactor, wOccupancyShare, "ln(CapFactor)", "ln(wOccupancyShare)")
print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# MAGIC %md
# MAGIC #### Office RemainingLeaseTerm

# COMMAND ----------

# DBTITLE 1,MV Plot wRemainingLeaseTerm - Level
dftmp <- rdf %>% filter(!is.na(wRemainingLeaseTerm)) %>% filter(PropertyType == 'Office')
mah.out <- mahalabonis.dist(dftmp, CapFactor, wRemainingLeaseTerm, "CapFactor", "wRemainingLeaseTerm")

print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# DBTITLE 1,MV Plot wRemainingLeaseTerm - Log-Log
dftmp <- rdf %>% filter(!is.na(wRemainingLeaseTerm)) %>% filter(PropertyType == 'Office')
dftmp$CapFactor <- log(dftmp$CapFactor)
dftmp$wRemainingLeaseTerm <- log1p(dftmp$wRemainingLeaseTerm)

mah.out <- mahalabonis.dist(dftmp, CapFactor, wRemainingLeaseTerm, "ln(CapFactor)", "ln(wRemainingLeaseTerm)")
print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# MAGIC %md
# MAGIC ## Industrial

# COMMAND ----------

# MAGIC %md
# MAGIC #### Industrial CreditScore

# COMMAND ----------

# DBTITLE 1,MV Plot wCreditScore - Level
dftmp <- rdf %>% filter(!is.na(wCreditScore)) %>% filter(PropertyType == 'Industrial')

mah.out <- mahalabonis.dist(dftmp, CapFactor, wCreditScore, "CapFactor", "wCreditScore")
print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# DBTITLE 1,MV Plot wCreditScore - Log-Log
dftmp <- rdf %>% filter(!is.na(wCreditScore)) %>% filter(PropertyType == 'Industrial')
dftmp$CapFactor <- log(dftmp$CapFactor)
dftmp$wCreditScore <- log(dftmp$wCreditScore)

mah.out <- mahalabonis.dist(dftmp, CapFactor, wCreditScore, "ln(CapFactor)", "ln(wCreditScore)")
print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# MAGIC %md
# MAGIC #### Industrial OccupancyShare

# COMMAND ----------

# DBTITLE 1,MV Plot wOccupancyShare - Level
dftmp <- rdf %>% filter(!is.na(wOccupancyShare)) %>% filter(PropertyType == 'Industrial')
mah.out <- mahalabonis.dist(dftmp, CapFactor, wOccupancyShare, "CapFactor", "wOccupancyShare")

print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# DBTITLE 1,MV Plot wOccupancyShare - Log-Log
dftmp <- rdf %>% filter(!is.na(wOccupancyShare)) %>% filter(PropertyType == 'Industrial')
dftmp$CapFactor <- log(dftmp$CapFactor)
dftmp$wOccupancyShare <- log1p(dftmp$wOccupancyShare)

mah.out <- mahalabonis.dist(dftmp, CapFactor, wOccupancyShare, "ln(CapFactor)", "ln(wOccupancyShare)")
print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# MAGIC %md
# MAGIC #### Industrial RemainingLeaseTerm

# COMMAND ----------

# DBTITLE 1,MV Plot wRemainingLeaseTerm - Level
dftmp <- rdf %>% filter(!is.na(wRemainingLeaseTerm)) %>% filter(PropertyType == 'Industrial')
mah.out <- mahalabonis.dist(dftmp, CapFactor, wRemainingLeaseTerm, "CapFactor", "wRemainingLeaseTerm")

print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# DBTITLE 1,MV Plot wRemainingLeaseTerm - Log-Log
dftmp <- rdf %>% filter(!is.na(wRemainingLeaseTerm)) %>% filter(PropertyType == 'Office')
dftmp$CapFactor <- log(dftmp$CapFactor)
dftmp$wRemainingLeaseTerm <- log1p(dftmp$wRemainingLeaseTerm)

mah.out <- mahalabonis.dist(dftmp, CapFactor, wRemainingLeaseTerm, "ln(CapFactor)", "ln(wRemainingLeaseTerm)")
print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retail

# COMMAND ----------

# MAGIC %md
# MAGIC #### Retail CreditScore

# COMMAND ----------

# DBTITLE 1,MV Plot wCreditScore - Level
dftmp <- rdf %>% filter(!is.na(wCreditScore)) %>% filter(PropertyType == 'Retail')

mah.out <- mahalabonis.dist(dftmp, CapFactor, wCreditScore, "CapFactor", "wCreditScore")
print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# DBTITLE 1,MV Plot wCreditScore - Log-Log
dftmp <- rdf %>% filter(!is.na(wCreditScore)) %>% filter(PropertyType == 'Retail')
dftmp$CapFactor <- log(dftmp$CapFactor)
dftmp$wCreditScore <- log(dftmp$wCreditScore)

mah.out <- mahalabonis.dist(dftmp, CapFactor, wCreditScore, "ln(CapFactor)", "ln(wCreditScore)")
print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# MAGIC %md
# MAGIC #### Retail OccupancyShare

# COMMAND ----------

# DBTITLE 1,MV Plot wOccupancyShare - Level
dftmp <- rdf %>% filter(!is.na(wOccupancyShare)) %>% filter(PropertyType == 'Retail')
mah.out <- mahalabonis.dist(dftmp, CapFactor, wOccupancyShare, "CapFactor", "wOccupancyShare")

print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# DBTITLE 1,MV Plot wOccupancyShare - Log-Log
dftmp <- rdf %>% filter(!is.na(wCreditScore)) %>% filter(PropertyType == 'Retail')
dftmp$CapFactor <- log1p(dftmp$CapFactor)
dftmp$wOccupancyShare <- log1p(dftmp$wOccupancyShare)

mah.out <- mahalabonis.dist(dftmp, CapFactor, wOccupancyShare, "ln(CapFactor)", "ln(wOccupancyShare)")
print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# MAGIC %md
# MAGIC #### Retail RemainingLeaseTerm

# COMMAND ----------

# DBTITLE 1,MV Plot wRemainingLeaseTerm - Level
dftmp <- rdf %>% filter(!is.na(wRemainingLeaseTerm)) %>% filter(PropertyType == 'Retail')
mah.out <- mahalabonis.dist(dftmp, CapFactor, wRemainingLeaseTerm, "CapFactor", "wRemainingLeaseTerm")

print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------

# DBTITLE 1,MV Plot wRemainingLeaseTerm - Log-Log
dftmp <- rdf %>% filter(!is.na(wRemainingLeaseTerm)) %>% filter(PropertyType == 'Retail')
dftmp$CapFactor <- log(dftmp$CapFactor)
dftmp$wRemainingLeaseTerm <- log1p(dftmp$wRemainingLeaseTerm)

mah.out <- mahalabonis.dist(dftmp, CapFactor, wRemainingLeaseTerm, "ln(CapFactor)", "ln(wRemainingLeaseTerm)")
print(mah.out$x.disc.5)
print(mah.out$x.disc.6)
mah.out$plot

# COMMAND ----------


