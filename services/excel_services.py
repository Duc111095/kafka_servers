from datetime import datetime
import os
import pandas as pd
from pathlib import Path
from utils.logger import get_app_logger

def create_excel_file_from_sql_data(cursor, query: str, src_file: str, start_row: int, dest_file_name: str = '') -> str:
    logger = get_app_logger()

    if not Path(os.path.dirname(os.path.dirname(__file__)) + "/source_excel" + "/" + src_file).is_file():
        print(os.path.dirname(os.path.dirname(__file__)) + "/source_excel" + "/" + src_file)
        raise FileNotFoundError(f"Source file {src_file} does not exist")
    if not src_file.endswith('.xlsx'):
        logger.error(f"Source file {src_file} is not an Excel file")
        return None
    
    src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "source_excel", src_file)
    content = pd.read_excel(src_path, header=None, skiprows=0)
    columns_dict = content.iloc[start_row + 1].to_dict()
    columns_header = content.iloc[start_row].to_dict()
    cursor.execute(query)
    index_table = get_table_index(str(columns_dict.get(0)))
    index_loop = 1

    while True: 
        if index_loop != index_table:
            if not cursor.nextset():
                break
            continue
        rows = cursor.fetchall()
        col_name_sql = [column[0] for column in cursor.description]
        for index, col in columns_dict.items():
            if not col:
                continue
            idx = start_row + 1
            col_name = get_column_name(str(col))
            for row in rows:
                if col_name and col_name in col_name_sql and col_name != '':  
                    content.at[idx, index] = row.__getattribute__(col_name)
                else:
                    content.at[idx, index] = col
                idx += 1
        index_loop += 1
        if not cursor.nextset():
            break
    if dest_file_name != '':
        dest_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "source_excel", dest_file_name)
    else:
        dest_file = src_path
    dest_file = dest_file.replace('.xlsx', '_' + datetime.now().strftime('%Y-%m-%d %f') + '.xlsx')
    save_excel_file(content, dest_file, columns_header)
    logger.info(f"Excel file created: {dest_file}")
    
    return dest_file


def get_table_index(s : str) -> int:
    try:
        i = s.index("!") + 1
        k = s.index(".")
        if (i >= k or i <= 0 or k < 0):
            return 0
        return int(s[i:k])
    except (IndexError, ValueError):
        return 0

def get_column_name(s : str) -> str:
    try:
        i = s.index(".") + 1
        k = len(s)
        return s[i:k].strip()
    except (IndexError, ValueError):
        return ''
    
def save_excel_file(df: pd.DataFrame, dest_file: str, columns_dict) -> str:
    logger = get_app_logger()

    with pd.ExcelWriter(dest_file, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Sheet1", index=False, header=False)
        worksheet = writer.sheets["Sheet1"]
        for i, col_name in columns_dict.items():
            if 'ngày' in col_name.lower() or 'date' in col_name.lower:
                df[i] = [element for element in df[i].astype(datetime).tolist()]
            elif 'giá' in col_name.lower() or 'tiền' in col_name.lower() or 'số lượng' in col_name.lower(): 
                df[i] = [element for element in df[i].astype(float).tolist()]
            else:
                df[i] = [element.strip() for element in df[i].astype(str).tolist()]

            column_len = df[i].map(len).max()
            column_len = max(column_len, len(col_name)) + 2
            worksheet.set_column(i, i, column_len)
    return dest_file