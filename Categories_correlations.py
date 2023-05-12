# Databricks notebook source
# MAGIC %sql Select * from  order_items_categories

# COMMAND ----------

df2 = spark.table("order_items_categories")
df2 =df2.dropna()

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml.stat import Correlation
from pyspark.ml import Pipeline

# Indexing 'category_name' column with  StringIndexer
string_indexer = StringIndexer(inputCol="category_name", outputCol="category_index")

# OneHotEncoder application to convert indexed columns into numeric columns
encoder = OneHotEncoder(inputCol="category_index", outputCol="category_onehot")

# Creating 2 stages pipeline: StringIndexer e OneHotEncoder
pipeline = Pipeline(stages=[string_indexer, encoder])

# Adjust pipeline and transforming Df
model = pipeline.fit(df2)
encoded_df = model.transform(df2)

# Selecting columns 'total_picking_time' e 'category_onehot' to calculate pearson correlation
correlation_matrix = Correlation.corr(encoded_df, "category_onehot", "pearson").collect()[0][0]

# COMMAND ----------

print(correlation_matrix)

# COMMAND ----------

import numpy as np

# Extracting correlations values and indexes 
correlations = [(i, correlation_matrix[i, 0]) for i in range(correlation_matrix.numRows)]

# Ordening values 
sorted_correlations = sorted(correlations, key=lambda x: abs(x[1]), reverse=True)

# Top 10 selection with indexes 
top_10_correlations = sorted_correlations[:10]

#Converting indexes to original names 
index_to_category = model.stages[0].labels
top_10_categories = [(index_to_category[index], corr_value) for index, corr_value in top_10_correlations]

# Print top 10 influent product categories
print("Top 10 most influent categories in total_picking_time:")
for category, corr_value in top_10_categories:
    print(f"{category}: {corr_value}")
