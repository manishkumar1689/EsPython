import pandas as pd

# Read data from Excel into a DataFrame
file_path = 'output2.xlsx'
df = pd.read_excel(file_path)

# Initialize map of map structure
map_of_map = {}
# Iterate through each row in the DataFrame
for index, row in df.iterrows():
    inner_map = {}

    # Example: Adding keys conditionally
    if 'name_en-us' in df.columns:
        inner_map["name_en-us"] = row['name_en-us']
    if 'name_zh-cn' in df.columns:
        inner_map["name_zh-cn"] = row['name_zh-cn']
    if 'name_zh-tw' in df.columns:
        inner_map["name_zh-tw"] = row['name_zh-tw']
    if 'name_ja-jp' in df.columns:
        inner_map["name_ja-jp"] = row['name_ja-jp']
    if 'name_ko-kr' in df.columns:
        inner_map["name_ko-kr"] = row['name_ko-kr']
    if 'name_th-th' in df.columns:
        inner_map["name_th-th"] = row['name_th-th']
    if 'name_id-id' in df.columns:
        inner_map["name_id-id"] = row['name_id-id']
    map_of_map[row['id']] = inner_map

# Print or use the map_of_map structure
print(map_of_map)


# Read from mapOfMap
example_id = 604  # Replace with an actual ID from your data
if example_id in map_of_map:
    inner_map = map_of_map[example_id]
    print(f"Name in English (en-us): {inner_map['name_en-us']}")
    print(f"Name in Chinese (zh-cn): {inner_map['name_zh-cn']}")
    # Access other fields similarly
else:
    print(f"ID {example_id} not found in the map of map.")
