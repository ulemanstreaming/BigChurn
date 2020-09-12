# BigChurn
Code for dealing with too much and too detailed data in a Data Science project for a churn use case

## Assets
1. Notebook [Generate a Subset Selector for a Large Dataset](Generate_Subset_By_Column_Spark.ipynb)

   A Spark-based notebook that supports creating a subset of a large dataset consisting of multiple source tables, each table split into multiple files (for, example, representing multiple time periods). The purpose of subsetting is to avoid getting bogged down in the highly iterative work of data exploration and feature engineering. More data may be better in training a model, but if it takes hours for every variation tried, it is difficult to make progress in coming up with the best approach.

   The use case that inspired this notebook was a supervised binary classification model for customer churn. Point the notebook at a table containing unique entity (customer or user) IDs and the class labels (for example, 0/1, True/False, or Yes/No); the result is a dataset, in the form of a partitioned Parquet file, with just two columns, unique ID and label, for a fraction of the IDs in the original dataset. In another notebook, read this file and use the resulting DataFrame to perform the actual subsetting of the source tables, in addition to any other necessary transformations and aggregations.

   The subsampling is randomized (seeded for repeatability) and with a configurable target class balance. In addition, it can take into account a minimum number periods during which a selected ID must have been present. (In other words, you need a certain number of periods of history to reveal a pattern that might predict churn.)

1. Notebook [Using Spark to Prepare a Large, Detailed Dataset](Subset_Transform_Aggregate_Combine_Files_Spark.ipynb)

   This notebook illustrates the following operations on a large dataset consisting of multiple files representing detailed records over several data collection periods for several distinct source tables.

   - Create a manageable subset of the full dataset by selecting specific unique IDs.     
   - Aggregate detail data.
   - Apply data transformations as needed.
   - Merge all files into a single DataFrame.
   
   The result is a single CSV file. This notebook relies on the [spark1csv.py](spark1csv.py) module.

1. Notebook [Prepare Fictitious Churn Data](Prepare%20Sample%20Churn%20Data.ipynb)

   This notebook uses a publicly available fictitious dataset for a telco churn use case to generate data structured as multiple monthly files of details on customers and calls.
   
   The prepared data files are intended as sample data for running the other notebooks in this repo. They are much too small to require any of the techniques demonstrated in those notebooks (they don't need Spark, for example), but they are enough to run the code and show the intended use and results.

1. Module [spark1csv.py](spark1csv.py) Write a Spark DataFrame into a single CSV file.
