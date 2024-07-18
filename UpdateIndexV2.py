import time
import logging
from elasticsearch import Elasticsearch, helpers
from openpyxl import Workbook
import pandas as pd

# Initialize workbook and sheet
workbook = Workbook()
sheet = workbook.active

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Elasticsearch Basic Auth credentials
USERNAME = 'elastic'
PASSWORD = 'IV9PCi6eyzE7W9PHy5gF'

# Elasticsearch connection setup with Basic Auth
es = Elasticsearch(['http://35.224.91.31:9200'],
                   basic_auth=(USERNAME, PASSWORD),
                   verify_certs=False)

# Excel file path
file_path = 'HotelContent.xlsx'

# Read Excel data into a DataFrame
df = pd.read_excel(file_path)

# Convert DataFrame to dictionary (HashMap)
#data_dict = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
map_of_map = {}
# Iterate through each row in the DataFrame
for index, row in df.iterrows():
    inner_map = {}

    # Example: Adding keys conditionally
    if 'city' in df.columns:
        inner_map["city"] = row['city']
    if 'country' in df.columns:
        inner_map["country"] = row['country']
    # print(f"row : {row}")
    if isinstance(row['aaId'],int):
        map_of_map[str(row['aaId'])] = inner_map
        print(f"first : {map_of_map[str(row['aaId'])]}")
        print(f"third : {row['aaId']}")
    else:
        map_of_map[row['aaId']] = inner_map
        print(f"second : { map_of_map[row['aaId']]}")
        print(f"third : {row['aaId']}")

# Initialize map_of_map dictionary
print(f"total docs:{len(map_of_map)}")
###############################################
# map_of_map = {}
#
# # First entry
# inner_map = {}
# inner_map["city"] = "Delhi"
# inner_map["country"] = "Ameria"
# row = {"aaId": "299e92"}
#
# map_of_map[row['aaId']] = inner_map
#
# # Second entry
# inner_map = {}
# inner_map["city"] = "Paros Island"
# inner_map["country"] = "Greece"
# row = {"aaId": "9888a"}
# map_of_map[row['aaId']] = inner_map
#
# # Second entry
# inner_map = {}
# inner_map["city"] = "Rohtak"
# inner_map["country"] = "India"
# row = {"aaId": 123}
#
# map_of_map[row['aaId']] = inner_map
#
# # List of keys to iterate over
# doc = ["299e92", "9888a",123,""]
#
# # Iterating over doc list
# for key in doc:
#     print(f"New way: {map_of_map.get(key)} {type(key)}")
# ################################################
#
# Elasticsearch index and query setup
index_a = "hotel-suggest-v8"
batch_size = 5000
index_a_query = {
    "query": {
        "bool": {
            "must": [{"match_all": {}}],
            "filter": [{"term": {"type": "hotel"}}]
        }
    },
    "size": batch_size
}


# Function to perform bulk updates
def bulk_update(updates, retries=3):
    attempt = 0
    while attempt < retries:
        try:
            helpers.bulk(es, updates)
            return True
        except Exception as e:
            logger.error(f"Bulk update attempt {attempt + 1} failed: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
            attempt += 1
    return False


# Main loop for scrolling and updating documents
scroll_id = None
batch_size = 1000

count=[]
while True:
    try:
        # Perform the initial search or continue scrolling
        if scroll_id is None:
            response = es.search(index=index_a, body=index_a_query, scroll='5m')
            scroll_id = response["_scroll_id"]
            index_a_results = response["hits"]["hits"]
        else:
            response = es.scroll(scroll_id=scroll_id, scroll='5m')
            index_a_results = response["hits"]["hits"]

        if not index_a_results:
            logger.info("No more hits returned by scroll.")
            break

        # Process documents and prepare updates
        updates = []
        for doc_a in index_a_results:
            try:
                if isinstance(doc_a["_source"]["aa_id"], int):
                    print(f"type first: {type(doc_a['_source']['aa_id'])}")
                    additional_info = map_of_map.get(str(doc_a["_source"]["aa_id"]), "")
                else:
                    print(f"type second: {type(doc_a['_source']['aa_id'])}")
                    additional_info = map_of_map.get(doc_a["_source"]["aa_id"], "")
                print(f"additional_info in map : {additional_info}")
                print(f"doc value : {doc_a['_source']['aa_id']}")
                print("#################################")
                update_doc = {
                    "_op_type": "update",
                    "_index": index_a,
                    "_id": doc_a["_id"],
                    "doc": {
                    }
                }
                if 'name_en-us' in doc_a['_source']:  # Check if the value is not None or empty
                    update_doc["doc"]["full_name_en-us"] = f"{doc_a['_source']['name_en-us']}, {additional_info['city']}, {additional_info['country']}"
                    print(f"update_doc : {update_doc['doc']['full_name_en-us']}")
                if update_doc["doc"] and additional_info:  # Only append if there are fields to update
                    print("doc is updated")
                    logger.debug(f"Prepared update: {update_doc}")
                    updates.append(update_doc)

            except KeyError as ke:
                logger.warning(f"KeyError: Missing key in document {doc_a['_id']}: {ke}")
            except ValueError as ve:
                logger.warning(f"ValueError: Unable to convert ID to integer for document {doc_a['_id']}: {ve}")
            except Exception as e:
                logger.error(f"Error processing document {doc_a['_id']}: {e}")

        # Execute bulk update
        if updates:
            success = bulk_update(updates)
            if not success:
                logger.error("Failed to perform bulk update after retries.")
                break

    except Exception as e:
        logger.error(f"Error during scrolling or updating: {e}")
        break

logger.info("Processing complete.")

# Save data to Excel file (optional)
# for row in output:
#     sheet.append(row)
# workbook.save(filename="output1.xlsx")
