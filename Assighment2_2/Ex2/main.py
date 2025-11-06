import requests
import pandas as pd
import os
from bs4 import BeautifulSoup
from datetime import datetime
import urllib.parse


def main(base_url: str, target_datetime_str: str, directory: str = "downloads") -> str:
    target_dt = datetime.strptime(target_datetime_str, "%Y-%m-%d %H:%M")
    os.makedirs(directory, exist_ok=True)

    with requests.Session() as session:
        # Fetch and parse page
        resp = session.get(base_url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')

        # Extract CSV links
        csv_urls = [
            urllib.parse.urljoin(base_url, a['href'])
            for a in soup.find_all('a', href=True)
            if a['href'].endswith('.csv')
        ]

        # Check each file's Last-Modified header
        for url in csv_urls:
            try:
                head_resp = session.head(url, timeout=10)
                head_resp.raise_for_status()
                last_mod_str = head_resp.headers.get('Last-Modified')
                if not last_mod_str:
                    continue

                last_mod_dt = datetime.strptime(last_mod_str, "%a, %d %b %Y %H:%M:%S %Z")
                if last_mod_dt.replace(second=0, microsecond=0) == target_dt:
                    # Download on match
                    get_resp = session.get(url, timeout=10)
                    get_resp.raise_for_status()
                    filename = os.path.join(directory, url.split('/')[-1])
                    with open(filename, 'wb') as f:
                        f.write(get_resp.content)
                    # print(f"Downloaded: {filename}")
                    return filename
            except Exception as e:
                print(f"Error: {url} - {e}")

        print("No file found with the specified timestamp.")
        return None

if __name__ == "__main__":
    result1 = main(
        "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/",
        "2024-01-19 15:45")   
    df = pd.read_csv(result1)
    print(df['HourlyDryBulbTemperature'].max())
