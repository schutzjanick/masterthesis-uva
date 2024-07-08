# Databricks notebook source
# MAGIC %md
# MAGIC _Masterthesis UvA - Janick Schutz - 2024_
# MAGIC # Regression Model: Breakdown Real Estate Segment Effects
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Imports
library(lfe)
library(dplyr)
library(ggplot2)
library(stargazer)

options(repr.plot.width = 800, repr.plot.height = 600)

# COMMAND ----------

# DBTITLE 1,Extract Prepared Data
# overall market dataset
source_path = "/dbfs/mnt/da-lab/users/janick_schutz/thesis/1_data_preparation/final.csv"
df = read.csv(source_path)
rdf = as.data.frame(df)
rdf$CapFactor <- rdf$CapFactorTRI  # CapFactorTRI / CapFactorERV
# display(rdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regression Model
# MAGIC Note that we use the lm regression rather with dummies for the fixed effects rather than a felm. This comes with a wide array of additional methods to diagnose the model fit.

# COMMAND ----------

# DBTITLE 1,Regression 1a: Office District FE
df1 <- rdf %>% filter(MunicipalityCode %in% c('362', '363', '394', '384', '437'))

mod1 <- lm(
  # dependent
  log(CapFactor)
  # variable of interest
  ~ log(wCreditScore)
  + log1p(wOccupancyShare)
  + log1p(wRemainingLeaseTerm)
  # controls
  + log(FloorArea)
  + I(PropertyType)
  + EffectiveAge
  + I(EnergyLabel)
  # fixed effects
  + I(ValuationYear)
  + I(DistrictCode),
  data = df1
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Identify outliers in Residuals
# MAGIC
# MAGIC Label outliers. `Note`: The `car` package gives error in the current version of R

# COMMAND ----------

# DBTITLE 1,CAR Package Outlier Test
car.outlier <- car::outlierTest(model)
car.outlier <- as.numeric(names(car.outlier[[1]])) 

df1$discordant_lm1[car.outlier] <- 1
df1$disc_fields[car.outlier] <- paste0(df1$disc_fields[car.outlier], 'residuals | ')
df1$disc_type[car.outlier] <- paste0(df1$disc_type[car.outlier], 'outliers car test | ')
remove(car.outlier)

# COMMAND ----------

# DBTITLE 1,Outliers based on Cook's Distance
cooksd <- cooks.distance(model)
influential <- as.numeric(names(cooksd)[(cooksd > 4*mean(cooksd, na.rm=T))])  # influential row numbers
df1$discordant_lm2[influential] <- 1
df1$disc_fields[influential] <- paste0(df1$disc_fields[influential], 'residuals | ')
df1$disc_type[influential] <- paste0(df1$disc_type[influential], 'cooksd | ')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Residual Analysis
# MAGIC
# MAGIC We investigate the residual plots of the fitted versus estimated values. 

# COMMAND ----------

# DBTITLE 1,Select Model
model <- mod1

# COMMAND ----------

# DBTITLE 1,Analyze Residuals
ggplot(model, aes(.fitted, .resid)) + geom_point() +
    stat_smooth(method="loess")+geom_hline(yintercept=0, col="red", linetype="dashed") +
    xlab("Fitted values")+ylab("Residuals") + theme_bw()

# COMMAND ----------

# DBTITLE 1,Analyze Standardized Residuals
ggplot(model, aes(qqnorm(.stdresid)[[1]], .stdresid))+geom_point(na.rm = TRUE) +
    geom_abline()+xlab("Theoretical Quantiles")+ylab("Standardized Residuals") +
    ggtitle("Normal Q-Q")+theme_bw()

# COMMAND ----------

# DBTITLE 1,Analyze Absolute Standardized Residuals
ggplot(model, aes(.fitted, sqrt(abs(.stdresid))))+geom_point(na.rm=TRUE) +
    stat_smooth(method="loess", na.rm = TRUE)+
    xlab("Fitted Value") +  ylab(expression(sqrt("|Standardized residuals|"))) + theme_bw()

# COMMAND ----------

# DBTITLE 1,Analyze Cook's Distance Observations
ggplot(model, aes(seq_along(.cooksd), .cooksd))+geom_bar(stat="identity", position="identity") +
    xlab("Obs. Number")+ylab("Cook's distance") + theme_bw() + ylim(0, 0.005)

# COMMAND ----------

# DBTITLE 1,Analyze Cook's Distance
ggplot(model, aes(.hat, .stdresid))+geom_point(aes(size=.cooksd), na.rm=TRUE) +
    stat_smooth(method="loess", na.rm=TRUE) +
    xlab("Leverage")+ylab("Standardized Residuals") +
    scale_size_continuous("Cook's Distance", range=c(1,5)) +
    theme_bw()+theme(legend.position="bottom") + xlim(0, 0.5)

# COMMAND ----------

# DBTITLE 1,Analyze Cook's Distance Leverage
ggplot(model, aes(.hat, .cooksd))+geom_point(na.rm=TRUE)+stat_smooth(method="loess", na.rm=TRUE) +
    xlab("Leverage hii")+ylab("Cook's Distance") +
    theme_bw()+ xlim(0, 0.5) + ylim(0, 0.015)

# COMMAND ----------

# MAGIC %md We eyeball potential remaining outliers in the residuals against independent variables.

# COMMAND ----------

# DBTITLE 1,wCreditScore
ggplot(model, aes(model[["model"]][["log(wCreditScore)"]], .resid)) + geom_point() +
  geom_hline(yintercept=0, col="red", linetype="dashed") +
  stat_smooth(method="loess") + theme_bw() + xlab("log(wCreditScore)")

# COMMAND ----------

# DBTITLE 1,wOccupancyShare
ggplot(model, aes(model[["model"]][["log1p(wOccupancyShare)"]], .resid)) + geom_point() +
  geom_hline(yintercept=0, col="red", linetype="dashed") +
  stat_smooth(method="loess") + theme_bw() + xlab("log1p(wOccupancyShare)")

# COMMAND ----------

# DBTITLE 1,wRemainingLeaseTerm
ggplot(model, aes(model[["model"]][["log1p(wRemainingLeaseTerm)"]], .resid)) + geom_point() +
  geom_hline(yintercept=0, col="red", linetype="dashed") +
  stat_smooth(method="loess") + theme_bw() + xlab("log1p(wRemainingLeaseTerm)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Diagnostics / Tests
# MAGIC
# MAGIC We now run some diagnostics on the model.
# MAGIC
# MAGIC We start by testing for **multicollinearity**. We check the variance inflation factors (VIF) and find that none of them exceed 10, the standard warning level.

# COMMAND ----------

vif(mod1)
sqrt(vif(mod1)) > 2

# COMMAND ----------

# MAGIC %md Next, we test for **heteroskedasticity** with two Breusch-Pagan test, one studentized, one not. Both indicate high levels of heteroskedasticity.

# COMMAND ----------

## studentized
bptest(mod1)

## not studentized
ncvTest(mod1)

# COMMAND ----------

# MAGIC %md Given this presence of heteroskeasticity, we apply **White's correction** to the standard errors to see if any of the previously significant variables undergo large changes in this significance levels.

# COMMAND ----------

coeftest(mod1, vcov=vcovHC(mod1, "HC1"))

# COMMAND ----------

# MAGIC %md We test for **Spatial Autocorrelation** in the residuals of our model.
# MAGIC
# MAGIC We begin with converting the transaction data to a *Spatial Point Data Frame*.

# COMMAND ----------

df1_spatial <- SpatialPointsDataFrame(coords=cbind(df1$longitude, df1$latitude), data=df1)

# COMMAND ----------

# MAGIC %md We then build a spatial weights matrix of the observations, using an *inverse distance-weighted*, 10-nearest neighbors matrix.

# COMMAND ----------

## Create neigbor list (use 10 nearest)
nbList <- knn2nb(knearneigh(df1_spatial, 10))

## create distances
nbDists <- nbdists(nbList, df1_spatial)

## Create a inverse distance weighting function (.0025 is the nugget)
dwf <- function(x) {1 / ((x + 0.0025) ^2)}

## Build the SWM
swm <- listw2U(nb2listw(nbList, 
                        glist= lapply(nbDists, dwf), 
                        style="W",
                        zero.policy = TRUE))

# COMMAND ----------

# MAGIC %md Using the spatial weights matrix, we then apply a **Moran's I** test. We find significant Spatial Autocorrelation.

# COMMAND ----------

## moran's I test
mi.test <- moran.test(df1_spatial$resid, swm, zero.policy = TRUE)

## see output
mi.test

# COMMAND ----------

# MAGIC %md Next, we use a Lagrange Multiplier test to determine if the **Spatial Dependence** is in the dependent variable (spatial lag SAR) or in the model errors (spatial error SER)

# COMMAND ----------

## Lagrange Multiplier test
lm.test <- lm.LMtests(mod1,
                      swm,
                      test=c("LMerr", "LMlag","RLMerr","RLMlag"))

## see output
lm.test
