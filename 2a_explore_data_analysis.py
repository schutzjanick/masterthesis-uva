# Databricks notebook source
# MAGIC %md
# MAGIC _Masterthesis UvA - Janick Schutz - 2024_
# MAGIC # Exploratory Data Analysis
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Install Packages
!pip install keplergl

# COMMAND ----------

# DBTITLE 1,Load Packages
from keplergl import KeplerGl

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import seaborn as sns

from statsmodels.stats.outliers_influence import variance_inflation_factor


# COMMAND ----------

# DBTITLE 1,Parameters
source_path = "/mnt/da-lab/users/janick_schutz/thesis/1_data_preparation"

df_final = spark.read.load(f"{source_path}/final")
df_final.createOrReplaceTempView("final")
pdf_final = df_final.toPandas()

# COMMAND ----------

# List of columns to exclude
columns_to_exclude = ['Latitude', 'Longitude', `CapFactorERV`, `wRemainingLeaseTerm2`, `wRemainingLeaseTerm_Agg`]

# Drop the columns to exclude from the DataFrame
pdf_modified = pdf_final.drop(columns=columns_to_exclude)

# Compute the correlation matrix
correlation_table = pdf_modified.corr()

# Round the correlation matrix
matrix = correlation_table.round(1)

# Apply styling for the background gradient
matrix_styled = matrix.style.background_gradient(cmap="Reds")

# Create a mask for the upper triangle
mask_heatmap = np.triu(np.ones_like(matrix, dtype=bool))

# Plot the heatmap
plt.figure(figsize=(8,8)) # width, height
sns.heatmap(matrix, vmin=-0.3, vmax=1, annot=True, fmt="0.1f", square=True, mask=mask_heatmap)
plt.show()


# COMMAND ----------

# Assuming 'pdf_final' is your DataFrame
numerical_variables = pdf_final.select_dtypes(include=['float64', 'int64']).columns

# Check for missing or infinite values
missing_values = pdf_final[numerical_variables].isnull().sum()
infinite_values = np.isinf(pdf_final[numerical_variables]).sum()

# Handle missing or infinite values
# Option 1: Remove rows with missing or infinite values
pdf_final_cleaned = pdf_final.dropna(subset=numerical_variables)
pdf_final_cleaned = pdf_final_cleaned.replace([np.inf, -np.inf], np.nan).dropna(subset=numerical_variables)

# Option 2: Impute missing values
pdf_final_imputed = pdf_final.fillna(pdf_final.mean())
pdf_final_imputed = pdf_final_imputed.replace([np.inf, -np.inf], np.nan).fillna(pdf_final_imputed.mean())

# Calculate VIF for cleaned or imputed DataFrame
vif_data = pd.DataFrame()
vif_data["Variable"] = numerical_variables
vif_data["VIF"] = [variance_inflation_factor(pdf_final_cleaned[numerical_variables].values, i) for i in range(len(numerical_variables))]

vif_data

# COMMAND ----------

# Assuming 'pdf_final' is your DataFrame
numerical_variables = pdf_final.select_dtypes(include=['float64', 'int64']).columns

# Exclude 'CapFactorERV' from the numerical variables
numerical_variables = numerical_variables.drop('CapFactorERV')

# Check for missing or infinite values
missing_values = pdf_final[numerical_variables].isnull().sum()
infinite_values = np.isinf(pdf_final[numerical_variables]).sum()

# Handle missing or infinite values
# Option 1: Remove rows with missing or infinite values
pdf_final_cleaned = pdf_final.dropna(subset=numerical_variables)
pdf_final_cleaned = pdf_final_cleaned.replace([np.inf, -np.inf], np.nan).dropna(subset=numerical_variables)

# Option 2: Impute missing values
pdf_final_imputed = pdf_final.fillna(pdf_final.mean())
pdf_final_imputed = pdf_final_imputed.replace([np.inf, -np.inf], np.nan).fillna(pdf_final_imputed.mean())

# Calculate VIF for cleaned or imputed DataFrame
vif_data = pd.DataFrame()
vif_data["Variable"] = numerical_variables
vif_data["VIF"] = [variance_inflation_factor(pdf_final_cleaned[numerical_variables].values, i) for i in range(len(numerical_variables))]

vif_data

# COMMAND ----------

import pandas as pd
import numpy as np
from statsmodels.stats.outliers_influence import variance_inflation_factor

# Assuming 'pdf_final' is your DataFrame
numerical_variables = pdf_final.select_dtypes(include=['float64', 'int64']).columns.tolist()

# List of variables to exclude from VIF calculation
variables_to_exclude = ['Latitude', 'Longitude', 'CapFactorERV', 'wRemainingLeaseTerm2', 'wRemainingLeaseTerm_Agg']

# Exclude the specified variables from the numerical variables
numerical_variables = [var for var in numerical_variables if var not in variables_to_exclude]

# Check for missing or infinite values
missing_values = pdf_final[numerical_variables].isnull().sum()
infinite_values = np.isinf(pdf_final[numerical_variables]).sum()

# Handle missing or infinite values
# Option 1: Remove rows with missing or infinite values
pdf_final_cleaned = pdf_final.dropna(subset=numerical_variables)
pdf_final_cleaned = pdf_final_cleaned.replace([np.inf, -np.inf], np.nan).dropna(subset=numerical_variables)

# Option 2: Impute missing values (using mean imputation as an example)
pdf_final_imputed = pdf_final.copy()
for col in numerical_variables:
    pdf_final_imputed[col].fillna(pdf_final[col].mean(), inplace=True)
    pdf_final_imputed[col].replace([np.inf, -np.inf], pdf_final[col].mean(), inplace=True)

# Choose the cleaned DataFrame for VIF calculation
df_for_vif = pdf_final_cleaned

# Calculate VIF for the selected DataFrame
vif_data = pd.DataFrame()
vif_data["Variable"] = numerical_variables
vif_data["VIF"] = [variance_inflation_factor(df_for_vif[numerical_variables].values, i) for i in range(len(numerical_variables))]

print(vif_data)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Univariate Analysis

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


# COMMAND ----------

source_path = "/mnt/da-lab/users/janick_schutz/thesis/1_data_preparation"

# source_path = "/mnt/da-lab/users/janick_schutz/thesis/0_data_collection"
# destination_path = "/mnt/da-lab/users/janick_schutz/thesis/1_data_preparation"

df_property = spark.read.load(f"{source_path}/property")
df_property.createOrReplaceTempView("property")



# COMMAND ----------

# Convert Spark DataFrame to Pandas DataFrame
pdf_property = df_property.toPandas()

# COMMAND ----------


# List of variables of interest FROM FINAL (Property Based)
variables = [
    'CapFactorERV', 'CapFactorTRI', 'ValuationYear', 'ValuationYear',  
    'FloorArea', 'EffectiveAge', 
]

# Use Seaborn's set_style for consistent aesthetics
sns.set_style("whitegrid")

# Loop through each variable
for var in variables:
    plt.figure(figsize=(10, 6))
    
    # Ensure the data is numeric and drop NaNs
    data = pd.to_numeric(pdf_final[var], errors='coerce').dropna()
    
    # Plot histogram using Seaborn
    sns.histplot(data, bins=30, color='b', kde=True, alpha=0.7)
    
    plt.title(f'Histogram of {var}')
    plt.xlabel(var)
    plt.ylabel('Frequency')
    
    # Add grid
    plt.grid(True)
    
    # Show the plot
    plt.show()


# COMMAND ----------


# List of variables of interest FROM FINAL (Property Based)
variable = 'PropertyType'

# Use Seaborn's set_style for consistent aesthetics
sns.set_style("whitegrid")

# Plot histogram for 'PropertyType'
plt.figure(figsize=(10, 6))

# Drop NaNs for 'PropertyType'
data = pdf_final[variable].dropna()

# Plot histogram using Seaborn
sns.histplot(data, bins=30, color='b', alpha=0.7, kde=True,)

plt.title(f'Histogram of {variable}')
plt.xlabel(variable)
plt.ylabel('Frequency')

# Add grid
plt.grid(True)

# Show the plot
plt.show()


# COMMAND ----------


# List of variables of interest from PROPERTY
variables = [
    'ConstructionYear' 
]

# Use Seaborn's set_style for consistent aesthetics
sns.set_style("whitegrid")

# Plot histogram for 'ConstructionYear'
for var in variables:
    plt.figure(figsize=(10, 6))

    # Drop NaNs for 'ConstructionYear'
    data = pdf_property[var].dropna()

    # Plot histogram using Seaborn
    sns.histplot(data, bins=30, color='b', kde=True, alpha=0.7)

    plt.title(f'Histogram of {var}')
    plt.xlabel(var)
    plt.ylabel('Frequency')

    # Add grid
    plt.grid(True)

    # Show the plot
    plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## logarithm

# COMMAND ----------

##LOG of FloorArea, CapFactorERV and CapFactorTRI 


# Apply logarithm to the 'FloorArea' variable, handling NaN values
log_floor_area = np.log(pdf_final['FloorArea'].dropna())

# Plot the histogram
plt.figure(figsize=(10, 6))
plt.hist(log_floor_area, bins=30, color='b', alpha=0.7)
plt.title('Histogram of Logarithm of Floor Area')
plt.xlabel('Log(Floor Area)')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()

# Apply logarithm to the 'CapFactorTRI' variable, handling NaN values
log_cap_factor_tri = np.log(pdf_final['CapFactorTRI'].dropna())

# Plot the histogram for 'CapFactorTRI'
plt.figure(figsize=(10, 6))
plt.hist(log_cap_factor_tri, bins=30, color='b', alpha=0.7)
plt.title('Histogram of Logarithm of CapFactorTRI')
plt.xlabel('Log(CapFactorTRI)')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()

# Apply logarithm to the 'CapFactorERV' variable, handling NaN values
log_cap_factor_erv = np.log(pdf_final['CapFactorERV'].dropna())

# Plot the histogram for 'CapFactorERV'
plt.figure(figsize=(10, 6))
plt.hist(log_cap_factor_erv, bins=30, color='b', alpha=0.7)
plt.title('Histogram of Logarithm of CapFactorERV')
plt.xlabel('Log(CapFactorERV)')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## per PropertyType

# COMMAND ----------


# Define the list of X variables
x_variables = [
    'wCreditScore', 'wRemainingLeaseTerm', 'wOccupancyShare', 
    'ValuationYear', 'FloorArea', 'EffectiveAge']
#Variables that do not work for some reason
# PropertyType = Not numerical , 
# LaborForce, YearFounded, YearConstructed, EnergyLabel = something wrong, look at it again after cleaning
# Loop through each X variable
for x_var in x_variables:
    # Drop NaNs for the current pair of variables
    df = pdf_final[[x_var, 'CapFactorTRI']].dropna()
    
    # Ensure data is numeric
    df[x_var] = pd.to_numeric(df[x_var], errors='coerce')
    df['CapFactorTRI'] = pd.to_numeric(df['CapFactorTRI'], errors='coerce')
    
    # Drop rows where conversion to numeric failed
    df = df.dropna()
    
    # Create a histogram with a KDE and scatter plot
    sns.histplot(data=df, x=x_var, y='CapFactorTRI', kde=True)
    plt.title(f'Histogram with Best Fit Line: {x_var} vs CapFactorTRI')
    plt.xlabel(x_var)
    plt.ylabel('CapFactorTRI')
    
    # Calculate the best fit line
    x = df[x_var]
    y = df['CapFactorTRI']
    m, b = np.polyfit(x, y, 1)
    plt.plot(x, m*x + b, color='red')
    
    # Show the plot
    plt.show()


# COMMAND ----------


# 'pdf_property' is the DataFrame and it contains 'ConstructionYear' and 'PropertyType'
property_types = pdf_property['PropertyType'].dropna().unique()

for property_type in property_types:
    # Filter the DataFrame for the current property type
    construction_years = pdf_property[pdf_property['PropertyType'] == property_type]['ConstructionYear'].dropna()
    
    # Plot the histogram
    plt.figure(figsize=(10, 6))
    plt.hist(construction_years, bins=30, color='b', alpha=0.7)
    plt.title(f'Histogram of Construction Year for {property_type}')
    plt.xlabel('Construction Year')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()


# COMMAND ----------


# 'pdf_final' as  DataFrame and it contains 'AvgCreditScore' and 'PropertyType'
property_types = pdf_final['PropertyType'].dropna().unique()

for property_type in property_types:
    # Filter the DataFrame for the current property type
    AvgCreditScore = pdf_final[pdf_final['PropertyType'] == property_type]['wCreditScore'].dropna()
    
    # Plot the histogram
    plt.figure(figsize=(10, 6))
    plt.hist(AvgCreditScore, bins=30, color='b', alpha=0.7)
    plt.title(f'Histogram of Average Credit Score for {property_type}')
    plt.xlabel('Average Credit Score')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()


# COMMAND ----------



# Query data with updated column names
df_dist = spark.sql("""
    SELECT PropertyType
    FROM hive_metastore.da_lab.thesis_jannick_property
""")

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = df_dist.toPandas()

# Plot histogram of PropertyType
df_pandas['PropertyType'].value_counts().plot(kind='bar', color='b', alpha=0.7)

# Set labels and title
plt.xlabel('Property Type')
plt.ylabel('Frequency')
plt.title('Histogram of Property Type')

plt.show()


# COMMAND ----------

source_path = "/mnt/da-lab/users/janick_schutz/thesis/0_data_collection"
destination_path = "/mnt/da-lab/users/janick_schutz/thesis/1_data_preparation"

spark.read.load(f"{source_path}/property").createOrReplaceTempView("raw_property")
spark.read.load(f"{source_path}/lease").createOrReplaceTempView("raw_lease")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multivariate Analysis

# COMMAND ----------


# Define the list of X variables
x_variables = [
    'wCreditScore', 'wRemainingLeaseTerm', 'wOccupancyShare', 
    'ValuationYear', 'FloorArea', 
    'EffectiveAge'
]

# Loop through each X variable
for x_var in x_variables:
    # Drop NaNs for the current pair of variables
    df = pdf_final[[x_var, 'CapFactorTRI']].dropna()
    
    # Ensure data is numeric
    df[x_var] = pd.to_numeric(df[x_var], errors='coerce')
    df['CapFactorTRI'] = pd.to_numeric(df['CapFactorTRI'], errors='coerce')
    
    # Drop rows where conversion to numeric failed
    df = df.dropna()
    
    # Create a histogram with a KDE and scatter plot
    sns.histplot(data=df, x=x_var, y='CapFactorTRI', kde=True)
    plt.title(f'Histogram with Best Fit Line: {x_var} vs CapFactorTRI')
    plt.xlabel(x_var)
    plt.ylabel('CapFactorTRI')
    
    # Calculate the best fit line
    x = df[x_var]
    y = df['CapFactorTRI']
    m, b = np.polyfit(x, y, 1)
    plt.plot(x, m*x + b, color='red')
    
    # Show the plot
    plt.show()


# COMMAND ----------



# Query data with updated column names
df_dist = spark.sql("""
    SELECT ConstructionYear, CapfactorTRI
    FROM hive_metastore.da_lab.thesis_jannick_property
""")

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = df_dist.toPandas()

# Ensure data is numeric and drop NaNs
df_pandas['ConstructionYear'] = pd.to_numeric(df_pandas['ConstructionYear'], errors='coerce')
df_pandas['CapfactorTRI'] = pd.to_numeric(df_pandas['CapfactorTRI'], errors='coerce')
df_pandas = df_pandas.dropna()

# Plot scatter plot of ConstructionYear vs. CapfactorTRI
plt.figure(figsize=(10, 6))
sns.scatterplot(x='ConstructionYear', y='CapfactorTRI', data=df_pandas, color='b', alpha=0.7)

# Set labels and title
plt.xlabel('Construction Year')
plt.ylabel('CapfactorTRI')
plt.title('Interaction of Construction Year and CapfactorTRI')

# Calculate the best fit line
x = df_pandas['ConstructionYear']
y = df_pandas['CapfactorTRI']
m, b = np.polyfit(x, y, 1)
plt.plot(x, m*x + b, color='red')

plt.grid(True)
plt.show()


# COMMAND ----------



# Query data with updated column names
df_dist = spark.sql("""
    SELECT ConstructionYear, CapfactorTRI
    FROM hive_metastore.da_lab.thesis_jannick_property
""")

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = df_dist.toPandas()

# Ensure data is numeric and drop NaNs
df_pandas['ConstructionYear'] = pd.to_numeric(df_pandas['ConstructionYear'], errors='coerce')
df_pandas['CapfactorTRI'] = pd.to_numeric(df_pandas['CapfactorTRI'], errors='coerce')
df_pandas = df_pandas.dropna()

# Apply logarithmic transformation to CapfactorTRI
df_pandas['LogCapfactorTRI'] = np.log(df_pandas['CapfactorTRI'])

# Plot scatter plot of ConstructionYear vs. LogCapfactorTRI
plt.figure(figsize=(10, 6))
sns.scatterplot(x='ConstructionYear', y='LogCapfactorTRI', data=df_pandas, color='b', alpha=0.7)

# Set labels and title
plt.xlabel('Construction Year')
plt.ylabel('Log of CapfactorTRI')
plt.title('Interaction of Construction Year and Log of CapfactorTRI')

# Calculate the best fit line for the log-transformed CapfactorTRI
x = df_pandas['ConstructionYear']
y = df_pandas['LogCapfactorTRI']
m, b = np.polyfit(x, y, 1)
plt.plot(x, m*x + b, color='red')

plt.grid(True)
plt.show()


# COMMAND ----------



# Query data with updated column names
df_dist = spark.sql("""
    SELECT ValuationDate, CapfactorTRI
    FROM hive_metastore.da_lab.thesis_jannick_property
""")

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = df_dist.toPandas()

# Plot scatter plot of ValuationDate vs. CapfactorTRI
plt.figure(figsize=(10, 6))
plt.scatter(df_pandas['ValuationDate'], df_pandas['CapfactorTRI'], color='b', alpha=0.7)

# Set labels and title
plt.xlabel('Valuation Date')
plt.ylabel('CapfactorTRI')
plt.title('Interaction of Valuation Date and CapfactorTRI')

plt.grid(True)
plt.show()


# COMMAND ----------



# Query data with updated column names
df_dist = spark.sql("""
    SELECT ConstructionYear, FloorArea
    FROM hive_metastore.da_lab.thesis_jannick_property
""")

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = df_dist.toPandas()

# Ensure data is numeric and drop NaNs
df_pandas['ConstructionYear'] = pd.to_numeric(df_pandas['ConstructionYear'], errors='coerce')
df_pandas['FloorArea'] = pd.to_numeric(df_pandas['FloorArea'], errors='coerce')
df_pandas = df_pandas.dropna()

# Plot scatter plot of ConstructionYear vs. FloorArea
plt.figure(figsize=(10, 6))
sns.scatterplot(x='ConstructionYear', y='FloorArea', data=df_pandas, color='b', alpha=0.7)

# Set labels and title
plt.xlabel('Construction Year')
plt.ylabel('Floor Area')
plt.title('Interaction of Construction Year and Floor Area')

# Calculate the best fit line
x = df_pandas['ConstructionYear']
y = df_pandas['FloorArea']
m, b = np.polyfit(x, y, 1)
plt.plot(x, m*x + b, color='red')

plt.grid(True)
plt.show()


# COMMAND ----------


# Query data with updated column names
df_dist = spark.sql("""
    SELECT ConstructionYear, AVG(FloorArea) AS MeanFloorArea
    FROM hive_metastore.da_lab.thesis_jannick_property
    GROUP BY ConstructionYear
""")

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = df_dist.toPandas()

# Ensure data is numeric and drop NaNs
df_pandas['ConstructionYear'] = pd.to_numeric(df_pandas['ConstructionYear'], errors='coerce')
df_pandas['MeanFloorArea'] = pd.to_numeric(df_pandas['MeanFloorArea'], errors='coerce')
df_pandas = df_pandas.dropna()

# Plot scatter plot of ConstructionYear vs. MeanFloorArea
plt.figure(figsize=(10, 6))
sns.scatterplot(x='ConstructionYear', y='MeanFloorArea', data=df_pandas, color='b', alpha=0.7)

# Set labels and title
plt.xlabel('Construction Year')
plt.ylabel('Mean Floor Area')
plt.title('Mean Floor Area by Construction Year')

# Calculate the best fit line
x = df_pandas['ConstructionYear']
y = df_pandas['MeanFloorArea']
m, b = np.polyfit(x, y, 1)
plt.plot(x, m*x + b, color='red')

plt.grid(True)
plt.show()


# COMMAND ----------



# Query data with updated column names
df_dist = spark.sql("""
    SELECT FileID, CapfactorTRI, CAST(ValuationDate AS STRING), Latitude, Longitude, PropertyType, ConstructionYear, FloorArea
    FROM hive_metastore.da_lab.thesis_jannick_property
""")

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = df_dist.toPandas()

# Filter out rows where 'ConstructionYear' or 'FloorArea' might be missing
df_pandas = df_pandas.dropna(subset=['ConstructionYear', 'FloorArea'])

# Ensure 'ConstructionYear' is an integer
df_pandas['ConstructionYear'] = df_pandas['ConstructionYear'].astype(int)

# Create bins for 20-year intervals, limiting construction year to maximum 2030
max_construction_year = 2030
bins = np.arange(df_pandas['ConstructionYear'].min(), max_construction_year + 1, 20)

# Cut data into bins
df_pandas['ConstructionYearBin'] = pd.cut(df_pandas['ConstructionYear'], bins, right=False)

# Group by bins and calculate mean 'FloorArea'
mean_floor_area_per_bin = df_pandas.groupby('ConstructionYearBin')['FloorArea'].mean()

# Plot bar chart
fig, ax = plt.subplots()
ax.bar(mean_floor_area_per_bin.index.astype(str), mean_floor_area_per_bin.values, color='b', alpha=0.7)

# Set labels and title
ax.set_xlabel('Construction Year Bin')
ax.set_ylabel('Mean Floor Area')
ax.set_title('Mean Floor Area Over 20-Year Intervals')

# Rotate x-axis labels for better readability
plt.xticks(rotation=45, ha='right')

plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporal Trends

# COMMAND ----------

# DBTITLE 1,Cap Factor Trend - Overall Market
# MAGIC %sql
# MAGIC SELECT CapFactorTRI, ValuationDate FROM hive_metastore.da_lab.thesis_jannick_property

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# Query data with updated column names
df_dist = spark.sql("""
    SELECT FileID, CapfactorTRI, CAST(ValuationDate AS DATE) AS ValuationDate
    FROM hive_metastore.da_lab.thesis_jannick_property
""")

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = df_dist.toPandas()

# Convert ValuationDate to datetime
df_pandas['ValuationDate'] = pd.to_datetime(df_pandas['ValuationDate'])

# Group by ValuationDate and calculate mean CapfactorTRI
mean_capfactor_tri = df_pandas.groupby('ValuationDate')['CapfactorTRI'].mean()

# Plot line chart
plt.figure(figsize=(10, 6))
plt.plot(mean_capfactor_tri.index, mean_capfactor_tri.values, color='b', marker='o', linestyle='-')

# Set labels and title
plt.xlabel('Valuation Date')
plt.ylabel('Mean Capfactor TRI')
plt.title('Trend of Mean Capfactor TRI Over Time')

plt.grid(True)
plt.xticks(rotation=45)

plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC '0363' = AMS, 
# MAGIC
# MAGIC '0599' = Rotterdam 
# MAGIC
# MAGIC '0344' = Utrecht
# MAGIC
# MAGIC '0518' = 's Gravenhage
# MAGIC
# MAGIC '0014' = Groningen 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CapFactorTRI, ValuationDate, MunicipalityCode
# MAGIC FROM hive_metastore.da_lab.thesis_jannick_property
# MAGIC WHERE MunicipalityCode IN ('0363', '0599', '0344', '0518', '0014')
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cap Factor Trend - Per Municipality
# MAGIC %sql
# MAGIC SELECT CapFactorTRI, ValuationDate, MunicipalityCode
# MAGIC FROM hive_metastore.da_lab.thesis_jannick_property
# MAGIC WHERE MunicipalityCode IN ('0363', '0599', '0344', '0518', '0014')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Distribution

# COMMAND ----------

# query data
df_map = spark.sql("""
    SELECT FileID, CapFactorTRI, CAST(ValuationDate AS STRING), Latitude, Longitude
    FROM hive_metastore.da_lab.thesis_jannick_property
""")
df_map.display()

# COMMAND ----------

map1 = KeplerGl()
map1.add_data(df_map.toPandas(), name="Valuations")
map1.config = {
    "version": "v1",
    "config": {
        "mapState": {
            "latitude": 52.33,
            "longitude":  4.87,
            "zoom": 14
        },
        "visState": {}
    }
}

displayHTML(map1._repr_html_()
        .decode("utf-8")
        .replace(".height||400", ".height||750")
)

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# query data
df_map = spark.sql("""
    SELECT FileID, CapFactorTRI, CAST(ValuationDate AS STRING), Latitude, Longitude, PropertyType
    FROM hive_metastore.da_lab.thesis_jannick_property
""")
df_map.display()

# COMMAND ----------

df_pandas = df_map.toPandas()

# COMMAND ----------

property_types_of_interest = ["Retail", "Industrial", "Office"]

# COMMAND ----------

def create_map(df, property_type):
    filtered_df = df[df['PropertyType'] == property_type]
    map_ = KeplerGl()
    map_.add_data(filtered_df, name=f"Valuations - {property_type}")
    map_.config = {
        "version": "v1",
        "config": {
            "mapState": {
                "latitude": 52.33,
                "longitude": 4.87,
                "zoom": 14
            },
            "visState": {}
        }
    }
    return map_

# COMMAND ----------

# query data
df_dist = spark.sql("""
    SELECT FileID, CapFactorTRI, CAST(ValuationDate AS STRING), Latitude, Longitude, PropertyType
    FROM hive_metastore.da_lab.thesis_jannick_property
""")
df_dist.display()

# COMMAND ----------

for property_type in property_types_of_interest:
    map1 = create_map(df_pandas, property_type)
    displayHTML(
        map1._repr_html_()
        .decode("utf-8")
        .replace(".height||400", ".height||750")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **OLD VERSIONS**
# MAGIC
# MAGIC

# COMMAND ----------

# List of variables of interest from FINAL Lease based, on Prperty aggregation
variables = [
    'AvgCreditRating', 'AvgLaborForce', 'AvgYearFounded', 'AvgOccupancyShare',  
    'AvgRemainingLeaseTerm'
]
# DominantBusinessActivity
# Assuming 'pdf_final' is your DataFrame
for var in variables:
    plt.figure(figsize=(10, 6))
    plt.hist(pdf_final[var].dropna(), bins=30, color='b', alpha=0.7)
    plt.title(f'Histogram of {var}')
    plt.xlabel(var)
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()

# COMMAND ----------

# DBTITLE 1,Descriptive Statistics
# MAGIC %sql
# MAGIC SELECT * FROM final
# MAGIC WHERE wCreditScore IS NOT NULL AND MunicipalityCode IN ('0362', '0363', '0394', '0384', '0437')
