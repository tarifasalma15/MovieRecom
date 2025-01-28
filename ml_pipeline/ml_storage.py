# 
# This file is used to handle data storage by saving the transformed datasets to CSV files.
#
# This file contains 1 function:
# 
# save_to_csv(dataframes: list) -> None: To save the dataframes to CSV files.
# 

from pyspark.sql.functions import col


def save_to_csv(dataframe: list, csv_path: list) -> None:
    """
    To save the dataframe to CSV file.
    """

    # Save the DataFrame as 1 CSV file, as Spark automatically partitions it if not mentioned
    dataframe.coalesce(1).write.parquet(csv_path, mode='overwrite')
    print(f"Data saved to {csv_path}")
