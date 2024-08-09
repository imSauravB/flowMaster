import pandas as pd
import json
import os
import csv
from typing import Dict, List, Union

def save_data(file_content: Union[List[Dict], Dict, List, str, pd.DataFrame], file_name: str, file_type: str, zone: str = "raw", context: str = "books") -> str:
    try:
        file_path = f"./data_lake/{zone}/{context}/{file_name}.{file_type}"
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
