# Databricks notebook source
# MAGIC %md
# MAGIC _Masterthesis UvA - Janick Schutz - 2024_
# MAGIC # Data Collection
# MAGIC ---
# MAGIC
# MAGIC This notebook composes the full dataset(s) used throughout the thesis. These data are mainly centered around the entities of a valuation executed by Cushman & Wakefield. For this study this data has been enriched with other infomation.
# MAGIC
# MAGIC The `property` entity is enriched with:
# MAGIC - Statistical regions: [PDOK download](https://www.pdok.nl/atom-downloadservices/-/article/cbs-gebiedsindelingen)
# MAGIC - Land registry: [BAG download](https://www.kadaster.nl/zakelijk/producten/adressen-en-gebouwen/bag-2.0-extract) 
# MAGIC - Energy labels: [EP-online download](https://ep-online.nl/)
# MAGIC
# MAGIC The `lease` entity is enriched with:
# MAGIC - Chamber of commerce: [KvK API](https://developers.kvk.nl/apis/zoeken)
# MAGIC - Graydon credit ratings: [Bulk request](https://graydon.nl/en/about-us)

# COMMAND ----------

# DBTITLE 1,Parameters
destination_path = "/mnt/da-lab/users/janick_schutz/thesis/0_data_collection"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Property Info

# COMMAND ----------

# DBTITLE 1,Compose Property Dataset
df_property = spark.sql("""
  SELECT
    -- identifiers
    dos.FileID,
    prp.DisplayAddress,
    prp.Geocode.RawPostalCode,
    prp.Geocode.RawCityName,
    prp.CadastrePropertyID,
    prp.CadastreAddressableObjectID,
    prp.Latitude,
    prp.Longitude,
    -- fixed-effects
    dos.ValuationDate,
    YEAR(dos.ValuationDate) AS ValuationYear,
    LEFT(prp.CadastrePropertyID, 4) AS MunicipalityCode,
    geo.DistrictCode,
    geo.DistrictName,
    -- dependent
    val.MarketValueNetAfterCorrectionsRounded / exp.EstimatedRentalValue AS CapFactorERV,
    val.MarketValueNetAfterCorrectionsRounded / exp.TheoreticalRentalIncome AS CapFactorTRI,
    -- controls
    prp.YearConstructed,
    prp.YearLastRenovated,
    prp.PropertyType,
    enl.EnergyLabel,
    COALESCE(prp.LettableFloorArea, exp.LettableFloorArea) AS FloorArea,
    -- other
    val.MarketValueNetAfterCorrectionsRounded,
    val.MarketValueGrossBeforeCorrections,
    exp.EstimatedRentalValue,
    exp.TheoreticalRentalIncome,
    exp.RemainingLeaseTermCriticalInclVacancy,
    exp.RemainingLeaseTermCriticalExclVacancy,
    fin.FileOriginPath
  FROM
    lab_nld.de_silver_view.val_excel_bog_dossier AS dos
  LEFT JOIN lab_nld.de_silver_view.val_excel_bog_property AS prp ON dos.FileID = prp.FileID
  LEFT JOIN lab_nld.de_silver_view.val_excel_bog_geography AS geo ON dos.FileID = geo.FileID
  LEFT JOIN lab_nld.de_silver_view.val_excel_bog_valuation_capitalization AS val ON dos.FileID = val.FileID
  LEFT JOIN lab_nld.de_silver_view.val_excel_bog_exploitation AS exp ON dos.FileID = exp.FileID
  LEFT JOIN lab_nld.de_silver_view.val_excel_bog_final_likelihood AS fin ON dos.FileID = fin.FileID
  LEFT JOIN lab_nld.de_silver_view.sus_ep_online_active_label_property AS enl ON prp.CadastrePropertyID = enl.CadastrePropertyID
  WHERE
      YEAR(ValuationDate) BETWEEN 2015 AND 2024
      AND (prp.Latitude IS NOT NULL AND prp.Longitude IS NOT NULL AND prp.Latitude > 2)
      AND TheoreticalRentalIncome BETWEEN 1000 AND 100000000
      AND EstimatedRentalValue BETWEEN 1000 AND 100000000
      AND MarketValueNetAfterCorrectionsRounded BETWEEN 10000 AND 1000000000
      AND MarketValueNetAfterCorrectionsRounded / EstimatedRentalValue BETWEEN 2 AND 60
      AND MarketValueNetAfterCorrectionsRounded / TheoreticalRentalIncome BETWEEN 2 AND 60
      AND prp.PropertyType IN ('Office', 'Retail', 'Industrial')
  ORDER BY dos.ValuationDate DESC, prp.DisplayAddress DESC
""").dropDuplicates(["FileID"])

print(df_property.count())
df_property.createOrReplaceTempView("property")
# display(df_property)

# COMMAND ----------

# DBTITLE 1,Sink Property Dataset to Storage
df_property.write \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .option("mergeSchema", True) \
    .save(f"{destination_path}/property")

print(f"Sucessfully sinked {df_property.count()} rows and {len(df_property.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lease Info

# COMMAND ----------

# DBTITLE 1,Compose Lease Dataset
df_lease = spark.sql("""
    SELECT
        -- identifiers
        lea.LeaseID,
        lea.FileID,
        prp.ValuationYear,
        COALESCE(lea.RawDisplayAddress, lea.RawSpaceName, lea.RawTenantBranche) AS Description,
        -- lease
        lea.RawTenantName,
        ROUND(lea.LettableFloorArea, 0) AS FloorArea,
        ROUND(lea.EstimatedRentalValue, 0) AS EstimatedRentalValue,
        lea.RemainingLeaseTermCritical AS RemainingLeaseTerm,
        kvk.ChamberOfCommerceRegistrationNumber AS RegistrationNumber,
        YEAR(kvk.ChamberOfCommerceRegistrationDate) AS RegistrationDate,
        kvk.OrganisationName,
        kvk.LegalFormShort AS LegalForm,
        kvk.LaborForce,
        kvk.SBIPrimaryBusinessActivity AS BusinessActivity,
        kvk.SBIPrimaryBusinessActivityCode AS BusinessActivityCode,
        rat.CreditScore
    FROM
        lab_nld.de_silver_curated.val_excel_bog_lease AS lea
    INNER JOIN property AS prp ON lea.FileID = prp.FileID
    LEFT JOIN (SELECT * FROM lab_nld.de_silver_curated.clt_kvk_api_zoeken WHERE Pos = 0) AS tmp
        ON CWHASH2(lea.RawTenantName) = tmp.HashID
    LEFT JOIN lab_nld.de_silver_curated.clt_kvk_api_basisprofiel AS kvk
        ON tmp.ChamberOfCommerceRegistrationNumber = kvk.ChamberOfCommerceRegistrationNumber
    LEFT JOIN lab_nld.de_silver_curated.clt_graydon_creditsafe_ratings AS rat
        ON CWHASH2(lea.RawTenantName) = CWHASH2(rat.RawTenantName)
    LEFT JOIN lab_nld.de_silver_view.val_excel_bog_final_likelihood AS fin
        ON lea.FileID = fin.FileID
""").dropDuplicates(["LeaseID"])

print(df_lease.count())
df_lease.createOrReplaceTempView("lease")
# display(df_lease)

# COMMAND ----------

# MAGIC %md
# MAGIC Based on the output above we can observe that tenants are sometimes spread over multiple rows. For this research we are interested in the aggregations of these tenants and thus below we aggregate by the combination of valuation and tenant

# COMMAND ----------

# DBTITLE 1,Aggregate Lease Dataset by Tenant
df_lease_agg = spark.sql("""
  SELECT
    lea.FileID,
    FIRST_VALUE(prp.ValuationYear, TRUE) AS ValuationYear,
    FIRST_VALUE(lea.Description, TRUE) AS Description,
    lea.RawTenantName,
    SUM(lea.FloorArea) AS FloorArea,
    SUM(lea.EstimatedRentalValue) AS EstimatedRentalValue,
    AVG(lea.RemainingLeaseTerm) AS RemainingLeaseTerm,
    AVG(lea.CreditScore) AS CreditScore,
    FIRST_VALUE(lea.RegistrationNumber, TRUE) AS RegistrationNumber,
    FIRST_VALUE(lea.RegistrationDate, TRUE) AS YearRegistered,
    FIRST_VALUE(lea.OrganisationName, TRUE) AS OrganisationNameKvK,
    FIRST_VALUE(lea.LegalForm, TRUE) AS LegalForm,
    FIRST_VALUE(lea.LaborForce, TRUE) AS LaborForce,
    FIRST_VALUE(lea.BusinessActivity, TRUE) AS BusinessActivity,
    FIRST_VALUE(lea.BusinessActivityCode, TRUE) AS BusinessActivityCode,
    -- measures
    ROUND(SUM(lea.FloorArea) / FIRST_VALUE(prp.FloorArea), 2) AS FloorAreaShare,
    ROUND(SUM(lea.EstimatedRentalValue) / FIRST_VALUE(prp.EstimatedRentalValue), 2) AS EstimatedRentalValueShare,
    COALESCE(FloorAreaShare, EstimatedRentalValueShare) AS OccupancyShare
  FROM
    lease AS lea
  INNER JOIN property AS prp ON lea.FileID = prp.FileID
  GROUP BY lea.FileID, lea.RawTenantName
""")

print(df_lease_agg.count())
df_lease_agg.createOrReplaceTempView("lease_agg")
# display(df_lease_agg)

# COMMAND ----------

# DBTITLE 1,Sink Lease Dataset to Storage
df_lease_agg.write \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .option("mergeSchema", True) \
    .save(f"{destination_path}/lease")

print(f"Sucessfully sinked {df_lease_agg.count()} rows and {len(df_lease_agg.columns)} columns")
