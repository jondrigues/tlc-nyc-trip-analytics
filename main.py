import requests
import shutil
import os
import pyarrow.parquet as pq

def download_tripdata(table_name, year, month ):
    r=requests.get(f"https://d37ci6vzurychx.cloudfront.net/trip-data/{table_name}_tripdata_{year}-{month}.parquet",
                   stream=True)

    create_dir(f"/Users/jondrigues/PycharmProjects/pythonProject/data/raw/{table_name}/{year}/{month}")

    with open(f'/Users/jondrigues/PycharmProjects/pythonProject/data/raw/{table_name}/{year}/{month}/'
              f'{table_name}_{year}-{month}.parquet','wb') as out_file:
        shutil.copyfileobj(r.raw, out_file)
    return r

def create_dir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    demo_dowload_list = [
        ["yellow", "2023", "01"],
        ["yellow", "2022", "12"],
        ["green", "2023", "01"],
        ["green", "2022", "12"],
        ["fhv", "2023", "01"],
        ["fhv", "2022", "12"],
        ["fhvhv", "2023", "01"]
    ]

    for demo_download in demo_dowload_list:
        download_tripdata(
            table_name=demo_download[0],
            year=demo_download[1],
            month=demo_download[2]
        )

    table_name ="yellow"
    year ="2023"
    month ="01"

    df = pq.read_table(f"/Users/jondrigues/PycharmProjects/pythonProject/data/raw/{table_name}/{year}/{month}"
                       f"/{table_name}_{year}-{month}.parquet")
    print(df.schema)