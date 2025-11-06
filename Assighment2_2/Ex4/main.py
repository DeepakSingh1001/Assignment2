import json
import os
import glob
import pandas as pd

def main():
    path = os.path.join(os.getcwd(), "Converted_to_csv")
    os.makedirs(path, exist_ok=True)

    files = glob.glob('data/**/*.json', recursive=True)
    for i in files:
        file_name_with_Ext = i.split('/')[-1]
        file_name = file_name_with_Ext.split('.')[0]
        with open(i) as f:
            d = json.load(f)
            
            n = pd.json_normalize(d)
            n.to_csv("Converted_to_csv/{}.csv".format(file_name),index=False)
            print("{} Converted to Csv".format(file_name))


if __name__ == "__main__":
    main()