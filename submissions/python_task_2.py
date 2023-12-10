import pandas as pd


def calculate_distance_matrix(df: pd.DataFrame) -> pd.DataFrame():
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """

    # Create a DataFrame with unique IDs
    unique_ids = sorted(set(df['id_start'].unique()) | set(df['id_end'].unique()))
    distance_matrix = pd.DataFrame(index=unique_ids, columns=unique_ids)

    # Set diagonal values to 0
    distance_matrix = distance_matrix.fillna(0)

    # Populate the distance matrix
    for _, row in df.iterrows():
        id_start, id_end, distance = row['id_start'], row['id_end'], row['distance']
        distance_matrix.at[id_start, id_end] = distance
        distance_matrix.at[id_end, id_start] = distance

    # Cumulative distances along known routes
    for k in unique_ids:
        for i in unique_ids:
            for j in unique_ids:
                if distance_matrix.at[i, j] == 0 and i != j:
                    if distance_matrix.at[i, k] != 0 and distance_matrix.at[k, j] != 0:
                        distance_matrix.at[i, j] = distance_matrix.at[i, k] + distance_matrix.at[k, j]

    return distance_matrix


def unroll_distance_matrix(df: pd.DataFrame) -> pd.DataFrame():
    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    unrolled_data = []

    for id_start in df.index:
        for id_end in df.columns:
            if id_start != id_end:
                distance = df.at[id_start, id_end]
                unrolled_data.append({'id_start': id_start, 'id_end': id_end, 'distance': distance})

    return pd.DataFrame(unrolled_data)


def find_ids_within_ten_percentage_threshold(df: pd.DataFrame, reference_id) -> pd.DataFrame():
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """

    reference_data = df[df['id_start'] == reference_id]

    # Calculate the average distance for the reference value
    average_distance = reference_data['distance'].mean()

    # Calculate the lower and upper bounds within 10% of the average
    lower_bound = average_distance * 0.9
    upper_bound = average_distance * 1.1

    # Filter DataFrame based on the threshold
    within_threshold = df[(df['distance'] >= lower_bound) & (df['distance'] <= upper_bound)]

    # Get unique values from the 'id_start' column and sort them
    result_ids = sorted(within_threshold['id_start'].unique())

    return pd.DataFrame(result_ids)


def calculate_toll_rate(df: pd.DataFrame) -> pd.DataFrame():
    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    df['moto'] = 0.8 * df['distance']
    df['car'] = 1.2 * df['distance']
    df['rv'] = 1.5 * df['distance']
    df['bus'] = 2.2 * df['distance']
    df['truck'] = 3.6 * df['distance']
    df = df[["id_start", "id_end", "moto", "car", "rv", "bus", "truck"]]
    return df


#
# def calculate_time_based_toll_rates(df) -> pd.DataFrame():
#     """
#     Calculate time-based toll rates for different time intervals within a day.
#
#     Args:
#         df (pandas.DataFrame)
#
#     Returns:
#         pandas.DataFrame
#     """

#     # Write your logic here

# df="C:\\Users\\91944\\Desktop\\MapUp-Data-Assessment-F\\datasets\\dataset-3.csv"
# df=pd.read_csv(df)

# distance_matrix=calculate_distance_matrix(df)
# print(distance_matrix)

# avg_distance_ids = find_ids_within_ten_percentage_threshold(unrolled_dataframe,1001400)
# print((avg_distance_ids))


# toll_rate = calculate_toll_rate(unrolled_dataframe)
# print(toll_rate)

# pd.set_option('display.max_columns', None)
# time_based_toll_rates = calculate_time_based_toll_rates((toll_rate))
# print(time_based_toll_rates)

