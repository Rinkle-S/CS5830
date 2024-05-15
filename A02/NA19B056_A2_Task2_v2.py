# Importing necessary libraries and modules
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
import geopandas as gpd
import apache_beam as beam
from collections import defaultdict
from airflow.sensors.filesystem import FileSensor
import shutil
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import glob

# Define the function to extract data frames
def extract_data(directory_path, output_dir, fields):
    # Function to extract monthly data
    def extract_monthly(date_str):
        date_components = date_str.split('-') + date_str.split('/') + date_str.split('T')
        for component in date_components:
            if component.isdigit() and 1 <= int(component) <= 12:
                return component
        return None

    # Function to convert field values to float
    def convert_to_float(df, fields):
        for field in fields:
            df[field] = pd.to_numeric(df[field], errors='coerce')
        return df

    # Function to extract monthly data
    def extract_monthly_data(data):
        grouped_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for row in data:
            lat_lon_key = (row[0], row[1])
            month = extract_monthly(row[2])
            for i, field in enumerate(fields, start=3):
                grouped_data[lat_lon_key][field][month].append(row[i])

        final_data = []
        for lat_lon, field_data in grouped_data.items():
            lat, lon = lat_lon
            field_ly_data = []
            for field, monthly_values in field_data.items():
                sorted_keys = sorted(monthly_values.keys())
                month_wise = [monthly_values[key] for key in sorted_keys]
                field_ly_data.append(month_wise)
            final_data.append((lat, lon, field_ly_data))
        return final_data

    # Check directory existence and create output directory if not exists
    if not os.path.exists(directory_path):
        raise ValueError(f"Directory {directory_path} does not exist.")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_files = glob.glob(directory_path + "/*.csv")
    df_list = []
    for file in all_files:
        df = pd.read_csv(file)
        filtered_df = df[["LATITUDE", "LONGITUDE", "DATE"] + fields]
        df_list.append(filtered_df)

    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df = convert_to_float(combined_df, fields)

    with beam.Pipeline() as pipeline:
        final_data = (
                pipeline
                | "Create PCollections" >> beam.Create([combined_df.values.tolist()])
                | "Extract Monthly Data" >> beam.Map(extract_monthly_data)
        )

        final_data_list = final_data | beam.combiners.ToList()
        result = final_data_list | beam.ParDo(lambda x: x[0])
        result | beam.io.WriteToText(output_dir + "/First_pipeline.txt")
    return output_dir


# Define the function to compute monthly averages
def compute_monthly_averages(data, fields):
    lat, lon, monthly_data = data
    averaged_data = []
    for field_values in monthly_data:
        field_averages = [np.nanmean(month) for month in field_values]
        averaged_data.append(field_averages)
    return lat, lon, averaged_data


# Define the function to compute averages dataframe
def compute_averages_dataframe(output_dir, fields, **kwargs):
    def parse_line(line):
        scope = {"nan": np.nan}
        return eval(line, scope)

    ti = kwargs['ti']
    pipeline_txt = ti.xcom_pull(task_ids='extract_files_task')
    file_path = os.path.join(pipeline_txt, os.listdir(pipeline_txt)[0])

    with beam.Pipeline() as pipeline:
        data = pipeline | beam.io.ReadFromText(file_path)
        transformed_data = (
                data
                | "Parse lines" >> beam.Map(parse_line)
                | "Compute monthly averages" >> beam.Map(compute_monthly_averages, fields)
        )
        transformed_data_json = transformed_data | beam.Map(lambda x: str(x))
        transformed_data_json | beam.io.WriteToText(output_dir + '/averages_output.txt')
    return output_dir

# Function to plot monthly field plots
def plot_data_from_file(output_dir, fields, **kwargs):
    def parse_line(line):
        # Replace 'nan' with np.nan
        line = line.replace('nan', 'np.nan')
        # Evaluate the line to get the tuple
        return eval(line)

    ti = kwargs['ti']
    pipeline_txt = ti.xcom_pull(task_ids='process_file')  # Pulling the tuple from create average dataset function
    file_path = os.path.join(pipeline_txt, os.listdir(pipeline_txt)[0])
    store_data = pd.DataFrame()
    store_data["LATITUDE"] = ''
    store_data["LONGITUDE"] = ''
    for field in fields:
        store_data[field] = ''

    with open(file_path, 'r') as file:
        for line in file:
            # Extract latitude, longitude, and values
            latitude, longitude, values = parse_line(line)
            store_data.loc[len(store_data)] = [latitude, longitude] + values

    # Create folder for each field in the output directory
    for field in fields:
        directory_path = os.path.join(output_dir, field)

        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            print(f"Directory '{directory_path}' created.")

            # Give write access to the directory
            try:
                os.chmod(directory_path, 0o777)  # 0o777 represents full permissions for owner, group, and others
                print("Write access granted to the directory.")
            except OSError as e:
                print(f"Error occurred while granting write access: {e}")

    # Create monthly plots
    for i in range(12):
        processed_data = pd.DataFrame()
        processed_data["LATITUDE"] = store_data['LATITUDE']
        processed_data["LONGITUDE"] = store_data['LONGITUDE']
        # Flatten the lists of values in each field
        for field in fields:
            temp = []
            for j in range(len(store_data[field])):
                if len(store_data[field][j]) > i:
                    temp.append(store_data[field][j][i])
                else:
                    temp.append(np.nan)
            processed_data[field] = temp  # Store only the data corresponding to a given month

        # Convert latitude and longitude to a GeoDataFrame
        gdf = gpd.GeoDataFrame(processed_data,
                               geometry=gpd.points_from_xy(processed_data.LONGITUDE, processed_data.LATITUDE))

        # Set up the plot
        for j, field in enumerate(fields):
            fig, ax = plt.subplots(figsize=(10, 8))

            # Plot the heatmap
            gdf.plot(column=field, cmap='viridis', linewidth=0.8, ax=ax, legend=True)
            ax.set_title(f"Heatmap for {field}")
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")
            ax.set_aspect('equal')

            # Save the plot as a .png file
            filename = os.path.join(output_dir, f"{field}/month{i + 1}.png")
            plt.savefig(filename)
            plt.close()


# Delete all the data files after processing
def delete_data(dir_lst):
    for folder_path in dir_lst:
        try:
            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            print(f"Contents inside '{folder_path}' deleted.")
        except Exception as e:
            print(f"Error occurred while deleting contents: {e}")


# Defining the variables in the program
year = 2008
fields = ["HourlyDryBulbTemperature", "HourlyDewPointTemperature", "HourlyPressureChange"]
zip_location = f"/home/rinkle_hp/airflow/Results/{year}.zip"
data_store_loc = f"/home/rinkle_hp/airflow/Results/{year}_data"
pipeline_loc_1 = "/home/rinkle_hp/airflow/pipeline/Initialpipeline"
pipeline_loc_2 = "/home/rinkle_hp/airflow/pipeline/Secondpipeline"
image_results = "/home/rinkle_hp/airflow/Results/Images"
archive_bash_file = "/home/rinkle_hp/airflow/dags/check_archive.sh"

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'processing_data_pipeline',
    default_args=default_args,
    description='A DAG to process meteorological data',
    schedule_interval='*/1 * * * *',
)

wait_for_archive = FileSensor(
    task_id='wait_for_archive',
    filepath=zip_location,
    timeout=5,
    poke_interval=1,
    retries=0,
    mode='poke',
    dag=dag,
)

check_validity_and_unzip = BashOperator(
    task_id='check_validity_and_unzip',
    bash_command=f'{archive_bash_file} {zip_location} {data_store_loc}',
    dag=dag,
)

extract_data_files = PythonOperator(
    task_id='extract_files_task',
    python_callable=extract_data,
    op_kwargs={'directory_path': data_store_loc ,"output_dir":pipeline_loc_1,"fields":fields},
    provide_context=True,
    do_xcom_push=True,
    dag=dag,
)

compute_data_files = PythonOperator(
    task_id='process_file',
    python_callable=compute_averages_dataframe,
    op_kwargs={"output_dir":pipeline_loc_2 ,"fields":fields},
    provide_context=True,
    do_xcom_push=True,
    dag=dag,
)

plot_data_files = PythonOperator(
    task_id='plot_data',
    python_callable=plot_data_from_file,
    op_kwargs={"output_dir":image_results ,"fields":fields},
    provide_context=True,
    do_xcom_push=True,
    dag=dag,
)

delete_data_files = PythonOperator(
    task_id='delete_data_file',
    python_callable=delete_data,
    op_kwargs={"dir_lst":[data_store_loc,pipeline_loc_1,pipeline_loc_2]},
    provide_context=True,
    do_xcom_push=True,
    dag=dag,
)

#Order for tasks
wait_for_archive >> check_validity_and_unzip >> extract_data_files >> compute_data_files >> plot_data_files >> delete_data_files
