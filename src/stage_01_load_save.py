from src.utils.all_utils import read_yaml, create_directory
import argparse
import pandas as pd
import os

def get_data(config_path):
    config = read_yaml(config_path)

    remote_data_path = config["data_source"]
    #df = pd.read_csv(remote_data_path, sep=",")
    df = pd.read_excel(remote_data_path)
    # converting dataframe column datatype
    df["datetime"] = pd.to_datetime(df.Date_of_Booking)
    # group by name and convert dates into individual columns
    grouped_df = df.sort_values("datetime", ascending=False
                                ).groupby("Profile ID")["Date_of_Booking"].apply(list).apply(pd.Series).reset_index()
    # truncate and rename columns
    grouped_df = grouped_df[["Profile ID", 0, 1]]
    grouped_df.columns = ["Profile ID", "most_recent", "second_most_recent"]

    # creating column with number of days between purchase
    grouped_df["Number_of_days"] = (grouped_df["most_recent"] - grouped_df["second_most_recent"]).dt.days

    # creating 90_days column
    grouped_df.loc[grouped_df['Number_of_days'] <= 90, '90_days_repeat'] = 1
    grouped_df.loc[grouped_df['Number_of_days'] > 90, '90_days_repeat'] = 0
    # creating 30_days column
    grouped_df.loc[grouped_df['Number_of_days'] <= 30, '30_days_repeat'] = 1
    grouped_df.loc[grouped_df['Number_of_days'] > 30, '30_days_repeat'] = 0

    # filling null values with zeros
    grouped_df.fillna(0, inplace=True)

    # merging two dataframes
    s1 = pd.merge(df, grouped_df, how='left', on=['Profile ID'])

    # converting column "Date_of_Booking_period" into year-month column as "Date_of_Booking"
    s1['Date_of_Booking_period'] = s1['Date_of_Booking'].dt.to_period('M')
    # converting data type of column Date_of_Booking_period into str for plotting bar chart
    s1["Date_of_Booking_period"] = s1.Date_of_Booking_period.map(str)

    # converting column "90_days_repeat" from float to int
    s1["90_days_repeat"] = s1["90_days_repeat"].map(int)

    # creating dataframe with columns required for building ML model
    s2 = s1[["Source", "Slot of Booking (Hour of the Day)", "Date_of_Booking", "Date_of_Service_Requested",
             "90_days_repeat"]]

    s2["Days_between_booking_service"] = (s2["Date_of_Service_Requested"] - s2["Date_of_Booking"]).dt.days

    # dropping columns Date_of_Booking and Date_of_Service_Requested
    s2.drop("Date_of_Booking", axis=1, inplace=True)
    s2.drop("Date_of_Service_Requested", axis=1, inplace=True)

    s3 = pd.get_dummies(s2, prefix="Source", drop_first=True)

    remote_data_path = config["data_source1"]
    s3.to_csv(remote_data_path)
    df = pd.read_csv(remote_data_path, sep=",")

    print(df.head())

    # save dataset in the local directory
    # create path to directory: artifacts/raw_local_dir/data.csv
    artifacts_dir = config["artifacts"]['artifacts_dir']
    raw_local_dir = config["artifacts"]['raw_local_dir']
    raw_local_file = config["artifacts"]['raw_local_file']

    raw_local_dir_path = os.path.join(artifacts_dir, raw_local_dir)

    create_directory(dirs= [raw_local_dir_path])

    raw_local_file_path = os.path.join(raw_local_dir_path, raw_local_file)
    
    df.to_csv(raw_local_file_path, sep=",", index=False)



if __name__ == '__main__':
    args = argparse.ArgumentParser()

    args.add_argument("--config", "-c", default="config/config.yaml")

    parsed_args = args.parse_args()

    get_data(config_path=parsed_args.config)


