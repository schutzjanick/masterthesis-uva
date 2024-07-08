# Databricks notebook source
# MAGIC %md
# MAGIC _Masterthesis UvA - Janick Schutz - 2024_
# MAGIC # Data Preparation
# MAGIC ---
# MAGIC
# MAGIC Hard Filters (Dependent, Variable of Interest, Space, Time):
# MAGIC
# MAGIC - **`ValuationDate`:**    Between 2015 and 2024 
# MAGIC   - Outliers: 110 removed (Just a few typos, not worthy to report on)
# MAGIC
# MAGIC - **`Geocoordinates`:**   Valid geocodes 
# MAGIC   - 94853 - 85617 = 9236 removed (could resolve some manually if time)
# MAGIC
# MAGIC - **`CapFactor`:**        Between 2 and 80 
# MAGIC   -  94853 - 94035 = 818 removed
# MAGIC
# MAGIC - **`PropertyType`** Office, Industrial, Retail
# MAGIC
# MAGIC
# MAGIC
# MAGIC Remaining observations: 84501
# MAGIC
# MAGIC Soft Filters (Dependent, Variable of Interest, Space, Time):
# MAGIC
# MAGIC - **`ConstructionYear`:**    Between 1600 and 2030 
# MAGIC   - Observation between 1200 and 1600 were replaced by 1600. 
# MAGIC   - The remaining missing or faulty observations for `YearConstructed` were replaced with the average value of `YearConstructed` for the municipality where the building is located.
# MAGIC   - There were 203 observations smaller than 1200, 60 observations between 1200 and 1599, and 12 observations above 2030, of which 4 can be clearly associated with typos.
# MAGIC
# MAGIC - **'RemainingLeaseTerm'** between 0 and 30
# MAGIC
# MAGIC - **`EffectiveAge`:** Between 0 and 100
# MAGIC   - When the EffectiveAge calculated from ValuationDate is less than 0, it is set to 0. If the EffectiveAge is greater than 100, it is set to 100.
# MAGIC   - If the calculated age is within the range of 0 to 100, the calculated value is used directly.
# MAGIC
# MAGIC - **`PropertyType`**: 
# MAGIC   - If the `PropertyType` field contained a null value, it was replaced with 'Unknown'.
# MAGIC | Category      | Count  |
# MAGIC |---------------|--------|
# MAGIC | Healthcare    | 4428   |
# MAGIC | Hospitality   | 2414   |
# MAGIC | Industrial    | 12885  |
# MAGIC | Land          | 92     |
# MAGIC | Leisure       | 400    |
# MAGIC | Office        | 17352  |
# MAGIC | Other         | 218    |
# MAGIC | Parking       | 72     |
# MAGIC | Residential   | 4480   |
# MAGIC | Retail        | 26995  |
# MAGIC | Social        | 2159   |
# MAGIC | Unknown       | 12266  |
# MAGIC
# MAGIC
# MAGIC - **`FloorArea`**: Between 100 and 150000 square meter
# MAGIC   - According to experts form Cushman and Wakefield it is highly unlikely that there are units smaller than 100 square meter in the dataset.
# MAGIC   - The maximum possible `FloorArea` is set to 150000 as this represents approximately the largest asset in the Dataset, the WTC property in Amsterdam Zuid-As.
# MAGIC   - Values outside this range were replaced by the nearest boundary.
# MAGIC
# MAGIC - **`RemainingLeaseTerm`**: Between 0 and 50
# MAGIC   - If RemainingLeaseTerm is negative then winzorised and replazed by 0.
# MAGIC   - If RemainingLeaseTerm is larger than 50 (was the case for 125 observation) then the values has been replaced by the Municipality Average RemainingLeaseTerm of that observation. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Parameters
source_path = "/mnt/da-lab/users/janick_schutz/thesis/0_data_collection"
destination_path = "/mnt/da-lab/users/janick_schutz/thesis/1_data_preparation"

spark.read.load(f"{source_path}/property").createOrReplaceTempView("raw_property")
spark.read.load(f"{source_path}/lease").createOrReplaceTempView("raw_lease")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Property Info
# MAGIC
# MAGIC The general property and valuation characteristics we get from the property and valuation tables. These contain info about ...

# COMMAND ----------

# DBTITLE 1,Preprocess Property Dataset
df_property = spark.sql("""
    SELECT
        -- identifiers
        FileID,
        DisplayAddress,
        RawPostalCode,
        RawCityName,
        CadastrePropertyID,
        CadastreAddressableObjectID,
        Latitude,
        Longitude,
        -- fixed-effects
        ValuationDate,
        YEAR(ValuationDate) AS ValuationYear,
        COALESCE(MunicipalityCode, '0000') AS MunicipalityCode,
        COALESCE(DistrictCode, 'WK000000') AS DistrictCode,
        COALESCE(DistrictName, 'Unknown') AS DistrictName,
        -- dependent
        ROUND(CASE
            WHEN CapFactorERV < 0 THEN 0
            WHEN CapFactorERV > 60 THEN 60
            ELSE CapFactorERV
        END, 2) AS CapFactorERV,
        ROUND(CASE
            WHEN CapFactorTRI < 0 THEN 0
            WHEN CapFactorTRI > 60 THEN 60
            ELSE CapFactorTRI
        END, 2) AS CapFactorTRI,
        -- controls
        CASE
            WHEN YearConstructed BETWEEN 1600 AND 2030 THEN YearConstructed
            WHEN YearConstructed BETWEEN 1200 AND 1600 THEN 1600
        END AS _tmp,
        ROUND(COALESCE(_tmp, AVG(YearConstructed) OVER (PARTITION BY MunicipalityCode)), 0) AS ConstructionYear,
        ROUND(CASE
            WHEN ConstructionYear IS NULL THEN YEAR(ValuationDate) - AVG(YearConstructed) OVER (PARTITION BY 1)
            WHEN YEAR(ValuationDate) - COALESCE(YearLastRenovated, ConstructionYear) < 0 THEN 0
            WHEN YEAR(ValuationDate) - COALESCE(YearLastRenovated, ConstructionYear) > 100 THEN 100
            ELSE YEAR(ValuationDate) - COALESCE(YearLastRenovated, ConstructionYear)
        END, 0) AS EffectiveAge,
        COALESCE(PropertyType, 'Unknown') AS PropertyType,
        ROUND(CASE
            WHEN FloorArea < 100 THEN 100
            WHEN FloorArea > 150000 THEN 150000
            ELSE FloorArea
        END, 0) AS FloorArea,
        COALESCE(EnergyLabel, 'Unknown') AS EnergyLabel,
        -- other info
        ROUND(MarketValueNetAfterCorrectionsRounded, 0) AS MarketValueNetAfterCorrectionsRounded,
        ROUND(MarketValueGrossBeforeCorrections, 0) AS MarketValueGrossBeforeCorrections,
        ROUND(EstimatedRentalValue, 0) AS EstimatedRentalValue,
        ROUND(TheoreticalRentalIncome, 0) AS TheoreticalRentalIncome,
        ROUND(CASE
            WHEN RemainingLeaseTermCriticalExclVacancy < 0 THEN 0
            WHEN RemainingLeaseTermCriticalExclVacancy > 20 THEN 20
            ELSE RemainingLeaseTermCriticalExclVacancy
        END, 2) AS RemainingLeaseTermExclVacancy,
        ROUND(CASE
            WHEN RemainingLeaseTermCriticalInclVacancy < 0 THEN 0
            WHEN RemainingLeaseTermCriticalInclVacancy > 20 THEN 20
            ELSE RemainingLeaseTermCriticalInclVacancy
        END, 2) AS RemainingLeaseTermInclVacancy,
        FileOriginPath
    FROM raw_property
    ORDER BY ValuationDate DESC, DisplayAddress DESC
""")

df_property.createOrReplaceTempView("property")
# display(df_property)

# COMMAND ----------

# DBTITLE 1,Sink Prepared Property Dataset
# output delta
df_property.write \
    .mode("overwrite")  \
    .option("overwriteSchema", True) \
    .option("mergeSchema",True) \
    .save(f"{destination_path}/property")

# output csv
df_property.toPandas().to_csv(f"/dbfs/{destination_path}/property.csv")

print(f"Sucessfully sinked {df_property.count()} rows and {len(df_property.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lease Info
# MAGIC
# MAGIC The lease details we get from...
# MAGIC

# COMMAND ----------

# DBTITLE 1,Preprocess Lease Dataset
df_lease = spark.sql("""
    SELECT
        -- identifiers
        lea.FileID,
        lea.Description,
        lea.RawTenantName,
        lea.OrganisationNameKvK,
        lea.RegistrationNumber,
        -- general
        ROUND(lea.FloorArea, 0) AS FloorArea,
        ROUND(lea.EstimatedRentalValue, 0) AS EstimatedRentalValue,
        -- variables of interest
        lea.LegalForm,
        ROUND(lea.CreditScore, 0) AS CreditScore,
        IF(lea.YearRegistered < 1900, 1900, lea.YearRegistered) AS RegistrationYear,
        NULLIF(lea.LaborForce, 0) AS LaborForce,
        lea.BusinessActivity,
        lea.BusinessActivityCode,
        ROUND(CASE
            WHEN lea.ValuationYear - RegistrationYear < 0 THEN 0
            ELSE lea.ValuationYear - RegistrationYear
        END, 0) AS OrganizationAge,
        ROUND(CASE
            WHEN lea.OccupancyShare < 0 THEN 0
            WHEN lea.OccupancyShare > 1 THEN 1  
            ELSE lea.OccupancyShare
        END, 1) AS OccupancyShare,
        ROUND(CASE
            WHEN lea.RemainingLeaseTerm < 0 THEN 0
            WHEN lea.RemainingLeaseTerm > 30 THEN 30  
            ELSE lea.RemainingLeaseTerm
        END, 1) AS RemainingLeaseTerm
    FROM
        raw_lease AS lea
    INNER JOIN property AS val ON lea.FileID = val.FileID
""")

df_lease.createOrReplaceTempView("lease")
# display(df_lease)

# COMMAND ----------

# DBTITLE 1,Sink Prepared Lease Dataset
# output delta
df_lease.write \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .option("mergeSchema",True) \
    .save(f"{destination_path}/lease")

# output csv
df_lease.toPandas().to_csv(f"/dbfs/{destination_path}/lease.csv")

print(f"Sucessfully sinked {df_lease.count()} rows and {len(df_lease.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Property Aggregation
# MAGIC
# MAGIC Since the dependent variable is on the property level we need to aggregate the lease information. We ...

# COMMAND ----------

df_final = spark.sql("""
    SELECT

        -- core
        prp.FileID,
        FIRST(prp.DisplayAddress) AS DisplayAddress,
        FIRST(prp.Latitude) AS Latitude,
        FIRST(prp.Longitude) AS Longitude,
        FIRST(prp.RawPostalCode) AS RawPostalCode,
        FIRST(prp.RawCityName) AS RawCityName,
        FIRST_VALUE(prp.CadastrePropertyID, TRUE) AS CadastrePropertyID,
        FIRST(prp.MunicipalityCode) AS MunicipalityCode,
        FIRST(prp.DistrictCode) AS DistrictCode,
        FIRST(prp.DistrictName) AS DistrictName,
        FIRST(prp.ValuationDate) AS ValuationDate,
        FIRST(prp.ValuationYear) AS ValuationYear,
        FIRST(prp.CapFactorTRI) AS CapFactorTRI,
    
        -- controls
        FIRST(prp.PropertyType) AS PropertyType,
        FIRST(prp.FloorArea) AS FloorArea,
        FIRST(prp.EffectiveAge) AS EffectiveAge,
        FIRST(prp.EnergyLabel) AS EnergyLabel,
    
        -- measures
        FIRST(lea.BusinessActivity) AS DominantBusinessActivity,
        AVG(lea.OccupancyShare) AS wOccupancyShare,
        ROUND(SUM(lea.CreditScore * lea.FloorArea)/SUM(lea.FloorArea * INT(lea.CreditScore IS NOT NULL)), 1) AS wCreditScore,
        ROUND(SUM(lea.LaborForce * lea.FloorArea)/SUM(lea.FloorArea * INT(lea.LaborForce IS NOT NULL)), 1) AS wLaborForce,
        ROUND(SUM(lea.OrganizationAge * lea.FloorArea)/SUM(lea.FloorArea * INT(lea.OrganizationAge IS NOT NULL)), 1) AS wOrganizationAge,

        -- robustness: alternative lease term measures
        FIRST(prp.RemainingLeaseTermInclVacancy) AS wRemainingLeaseTerm,  -- base
        FIRST(prp.RemainingLeaseTermExclVacancy) AS wRemainingLeaseTerm2,  -- robust
        ROUND(SUM(lea.RemainingLeaseTerm * lea.FloorArea)/SUM(lea.FloorArea * INT(lea.RemainingLeaseTerm IS NOT NULL)), 1) AS wRemainingLeaseTerm_Agg,

        -- robustness: alternative dependent
        FIRST(prp.CapFactorERV) AS CapFactorERV,

        -- robustness: discretize for non-linearity
        CASE
            WHEN wCreditScore = 0 THEN "E"
            WHEN wCreditScore < 30 THEN "D"
            WHEN wCreditScore < 50 THEN "C"
            WHEN wCreditScore < 70 THEN "B"
            WHEN wCreditScore <= 100 THEN "A"
            ELSE "Unknown"
        END AS CatCreditScore,
        CASE
            WHEN wOccupancyShare < 80 THEN "Multi-Tenant"
            WHEN wOccupancyShare < 100 Then "Dominant-Tenant"
            WHEN wOccupancyShare = 100 THEN "Single-Tenant"
            ELSE "Unknown"
        END AS CatOccupancyShare,
        CASE
            WHEN wLaborForce <= 50 THEN "small"
            WHEN wLaborForce > 50 AND wLaborForce <= 500 THEN "medium"
            WHEN wLaborForce > 500 THEN "large"
            ELSE "unknown"
        END AS CatLaborForce,
        CASE
            WHEN wOrganizationAge <= 3 THEN "young"
            WHEN wOrganizationAge > 3 AND wOrganizationAge <= 10 THEN "medium"
            WHEN wOrganizationAge > 10 THEN "mature"
            ELSE "unknown"
        END AS CatOrganizationAge,
        CASE
            WHEN wRemainingLeaseTerm <= 3 THEN "short"
            WHEN wRemainingLeaseTerm > 3 AND wRemainingLeaseTerm <= 5 THEN "average"
            WHEN wRemainingLeaseTerm > 5 THEN "long"
            ELSE "unknown"
        END AS catRemainingLeaseTerm

    FROM
        property AS prp
    INNER JOIN lease AS lea ON prp.FileID = lea.FileID
    WHERE CadastrePropertyID IS NOT NULL
    GROUP BY prp.FileID
    ORDER BY CadastrePropertyID, ValuationDate
""")

df_final.createOrReplaceTempView("final")
# display(df_final)

# COMMAND ----------

# DBTITLE 1,Sink Final Dataset
# output delta
df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .option("mergeSchema",True) \
    .save(f"{destination_path}/final")

# output csv
df_final.toPandas().to_csv(f"/dbfs/{destination_path}/final.csv")

print(f"Sucessfully sinked {df_final.count()} rows and {len(df_final.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Repeated Valuations
# MAGIC
# MAGIC We also look specifically at properties appraised more than once...

# COMMAND ----------

df_final_repeat = spark.sql("""
    WITH
    dedup AS (
        SELECT * FROM final
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CadastrePropertyID, ValuationYear ORDER BY FileID) = 1
    ),

    repeated AS (
        SELECT CadastrePropertyID, COUNT(*) AS count FROM dedup
        GROUP BY CadastrePropertyID
    )

    SELECT dedup.* FROM dedup
    LEFT JOIN repeated ON dedup.CadastrePropertyID = repeated.CadastrePropertyID
    WHERE repeated.count > 1
    ORDER BY CadastrePropertyID, ValuationYear
""")

# display(df_final_repeat)

# COMMAND ----------

# DBTITLE 1,Sink Final Repeated Dataset
# output delta
df_final_repeat.write \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .option("mergeSchema", True) \
    .save(f"{destination_path}/final_repeat")

# output csv
df_final.toPandas().to_csv(f"/dbfs/{destination_path}/final_repeat.csv")

print(f"Sucessfully sinked {df_final_repeat.count()} rows and {len(df_final_repeat.columns)} columns")

# COMMAND ----------


