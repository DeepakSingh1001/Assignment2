import subprocess
import gzip

header = "https://data.commoncrawl.org/"
key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

def main():
    subprocess.run(["wget", header + key])
    
    first_file_name = key.split('/')[-1]
    
    with gzip.open(first_file_name, 'rt', encoding='utf-8') as f:
        first_file_data = f.read()

    second_file_key = first_file_data.strip().split('\n')[0]
    second_file_name = second_file_key.split('/')[-1]
    

    subprocess.run(["wget", header + second_file_key])
    
    with gzip.open(second_file_name, 'rt', encoding='utf-8') as f:
        for i, line in enumerate(f):
            print(line.strip())
    print("\n")

if __name__ == "__main__":
    main()   
