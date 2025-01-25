# 
# This file is used to handle data storage by saving the transformed datasets to CSV files.
#
# This file contains 1 function:
# 
# save_to_csv(dataframes: list, csv_paths: list) -> None: To save the dataframes to CSV files.
# 


import os

def save_to_csv(dataframe: list, csv_path: list) -> None:
    """
    To save the dataframe to CSV file.
    """

    # Create the directory if it does not exist
    directory = "datasets"
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Save the DataFrame as 1 CSV file, as Spark automatically partitions it if not mentioned
    dataframe.coalesce(1).write.csv(csv_path, header=True, mode='overwrite')
