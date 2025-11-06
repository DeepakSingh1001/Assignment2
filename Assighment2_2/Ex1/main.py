import requests
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


# def main():
#     download_dir="downloads"
#     # Create the downloads directory if it doesn't exist
#     os.makedirs(download_dir, exist_ok=True)
#     urls = download_uris
#     for url in urls:
#         try:
#             # Extract the filename from the url
#             filename = url.split('/')[-1]
#             zip_path = os.path.join(download_dir, filename)
    
#             print(f"Downloading {filename}---")
    
#             response = requests.get(url, stream=True)
#             if response.status_code == 200:
#                 with open(zip_path, 'wb') as f:
#                     for chunk in response.iter_content(chunk_size=1024):
#                         f.write(chunk)

#                 with zipfile.ZipFile(zip_path, 'r') as zip_ref:
#                     # Look for the .csv file inside the zip and extract it
#                     for member in zip_ref.namelist():
#                         if member.endswith('.csv'):
#                             zip_ref.extract(member, download_dir)
#                             print(f"Extracted {member}.")
#                             break
                
#                 # Delete the original zip file
#                 os.remove(zip_path)
#             else:
#                 print(f"Failed to download {filename}. Status code: {response.status_code}")
#         except Exception as e:
#             print(f"An error occurred while processing {url}: {e}")

def main():
    download_dir = "downloads"
    os.makedirs(download_dir, exist_ok=True)

    def download_and_extract(url):
        try:
            filename = url.split('/')[-1]
            zip_path = os.path.join(download_dir, filename)
            print(f"Downloading {filename}...")
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        f.write(chunk)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    for member in zip_ref.namelist():
                        if member.endswith('.csv'):
                            zip_ref.extract(member, download_dir)
                            print(f"Extracted {member}.")
                            break
                os.remove(zip_path)
            else:
                print(f"Failed to download {filename}. Status code: {response.status_code}")
        except Exception as e:
            print(f"Error processing {url}: {e}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_and_extract, download_uris)

if __name__ == "__main__":
    main()
