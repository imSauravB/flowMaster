import pandas as pd
import json
import os
import csv
from typing import Dict, List, Union
from datetime import datetime

def save_data(file_content: Union[List[Dict], Dict, List, str, pd.DataFrame], file_name: str, file_type: str, base_path="./data", zone: str = "raw", context: str = "books") -> str:
    try:
        current_date = datetime.now().strftime("%Y%m%d-%H-%M-%S")
        dir_path = f"{base_path}/{zone}/{context}/{current_date}"
        print(f'Going to create this dir:  {dir_path}')
        if not os.path.isdir(dir_path):
            os.mkdir(dir_path)
        file_path = f"{dir_path}/{file_name}.{file_type}"
        print(f'{file_path}')
        if file_type == "json":
            with open(file_path, 'w+') as f:
                json.dump(file_content, f, indent=4)
        
        elif file_type == "csv":
            if isinstance(file_content, pd.DataFrame):
                file_content.to_csv(file_path, index=False)
            else:
                df = pd.DataFrame(file_content)
                df.to_csv(file_path, index=False)
        
        elif file_type == "txt":
            with open(file_path, 'w') as f:
                if isinstance(file_content, list):
                    f.write('\n'.join(map(str, file_content)))
                else:
                    f.write(str(file_content))
        
        elif file_type == "parquet":
            df = pd.DataFrame(file_content)
            df.to_parquet(file_path, index=False)
        
        else:
            return f"Unsupported file type: {file_type}"
        
        return f"File saved successfully to {file_path}"
    except Exception as e:
        return f"An error occurred while saving the file: {e}"
