
import pandas as pd
from typing import Dict, List


def generate_car_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a DataFrame  for id combinations.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Matrix generated with 'car' values,
                          where 'id_1' and 'id_2' are used as indices and columns respectively.
    """

    car_matrix = df.pivot(index="id_1", columns="id_2", values="car")
    for col in car_matrix.columns:
        car_matrix.at[col, col] = 0
    return car_matrix


def get_type_count(df: pd.DataFrame) -> Dict:
    """
    Categorizes 'car' values into types and returns a dictionary of counts.

    Args:
        df (pandas.DataFrame)

    Returns:
        dict: A dictionary with car types as keys and their counts as values.
    """
    def find_car_type(val):
        if val <= 15:
            return "low"
        elif val <= 25:
            return "medium"
        else:
            return "high"

    # adding categorical column
    df["car_type"] = df["car"].apply(find_car_type)

    # finding no of occurrences
    type_counts = df["car_type"].value_counts().to_dict()

    # sorting dic based on keys
    sorted_type_counts = dict(sorted(type_counts.items()))
    return sorted_type_counts


def get_bus_indexes(df: pd.DataFrame) -> List:
    """
    Returns the indexes where the 'bus' values are greater than twice the mean.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of indexes where 'bus' values exceed twice the mean.
    """
    # finding bus indices that are twice the mean bus value
    bus_mean = df["bus"].mean()
    indexes = df[df["bus"] > 2 * bus_mean].index.tolist()

    # sort values on index
    indexes.sort()
    return indexes


def filter_routes(df: pd.DataFrame) -> List:
    """
    Filters and returns routes with average 'truck' values greater than 7.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of route names with average 'truck' values greater than 7.
    """
    truck_mean = df.groupby('route')["truck"].mean()

    # avg truck value > 7
    route_name = truck_mean[truck_mean > 7].index.tolist()
    route_name.sort()

    return route_name


def multiply_matrix(matrix: pd.DataFrame) -> pd.DataFrame:
    """
    Multiplies matrix values with custom conditions.

    Args:
        matrix (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Modified matrix with values multiplied based on custom conditions.
    """
    # Create a copy of the DataFrame to avoid modifying the original
    modified_matrix = matrix.copy()

    # Apply the specified logic to modify values
    modified_matrix[modified_matrix > 20] *= 0.75
    modified_matrix[modified_matrix <= 20] *= 1.25

    # Round values to 1 decimal place
    modified_matrix = modified_matrix.round(1)

    return modified_matrix


def time_check(df: pd.DataFrame) -> pd.Series:
    """
    Use shared dataset-2 to verify the completeness of the data by checking whether the timestamps for each unique (`id`, `id_2`) pair cover a full 24-hour and 7 days period

    Args:
        df (pandas.DataFrame)

    Returns:
        pd.Series: return a boolean series
    """
    # Write your logic here
    # Combine date and time columns to create a datetime column
    df['timestamp'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'], format='%A %H:%M:%S')

    # Group by (id, id_2) pairs and check completeness using a custom aggregation function
    result_series = df.groupby(['id', 'id_2']).apply(
        lambda group: (
                group['timestamp'].min() == pd.Timestamp('00:00:00') and
                group['timestamp'].max() == pd.Timestamp('23:59:59') and
                set(group['timestamp'].dt.date) == set(pd.date_range(start=group['timestamp'].min().date(), end=group['timestamp'].max().date(), freq='D')) and
                set(group['timestamp'].dt.day_name()) == {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday',
                                                          'Saturday', 'Sunday'}
                    )
            )
    return pd.Series(result_series)

# test
# excel_path_dataset2 = "C:\\Users\\91944\\Desktop\\MapUp-Data-Assessment-F\\datasets\\dataset-2.csv"
# test_df_dataset2 = pd.read_csv(excel_path_dataset2)
# check_time = time_check(test_df_dataset2)
# print(check_time)
# indices_list = get_bus_indexes(test_df_dataset2)
# print(indices_list)
# routes_filter = filter_routes(test_df_dataset2)
# print(routes_filter)
# mat_mul= multiply_matrix(test_df_dataset2)
# print(mat_mul)
#
# excel_path_dataset1 = "C:\\Users\\91944\\Desktop\\MapUp-Data-Assessment-F\\datasets\\dataset-1.csv"
# test_df = pd.read_csv(excel_path_dataset1)
# car = generate_car_matrix(test_df)
# print(car)





