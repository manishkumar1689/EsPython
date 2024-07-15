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
                   http_auth=(USERNAME, PASSWORD),
                   scheme='http',
                   port=9200,
                   verify_certs=False)

# Excel file path
file_path = 'output.xlsx'

# Read Excel data into a DataFrame
df = pd.read_excel(file_path, header=None, engine='openpyxl')

# Convert DataFrame to dictionary (HashMap)
#data_dict = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
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
                additional_info = map_of_map.get(int(doc_a["_source"]["id"]), "")

                update_doc = {
                    "_op_type": "update",
                    "_index": index_a,
                    "_id": doc_a["_id"],
                    "doc": {}
                }

                # Define fields to check and update
                fields_to_update = [
                    # "keyword_full_name_en-us",
                    "keyword_full_name_zh-cn",
                    "keyword_full_name_zh-tw",
                    "keyword_full_name_ja-jp",
                    "keyword_full_name_ko-kr",
                    "keyword_full_name_th-th",
                    "keyword_full_name_id-id"
                ]

                keyValue = {
                    # "keyword_full_name_en-us": "full_name_en-us",
                    "keyword_full_name_zh-cn": "full_name_zh-cn",
                    "keyword_full_name_zh-tw": "full_name_zh-tw",
                    "keyword_full_name_ja-jp": "full_name_ja-jp",
                    "keyword_full_name_ko-kr": "name_ko-kr",
                    "keyword_full_name_th-th": "name_th-th",
                    "keyword_full_name_id-id": "name_id-id"
                }

                # Iterate over fields and add to update_doc if they have a value
                for field in fields_to_update:
                    value = doc_a["_source"].get(field)
                    if value and additional_info[keyValue[field]]:  # Check if the value is not None or empty
                        update_doc["doc"][field] = f"{value}, {additional_info[keyValue[field]]}"

                if update_doc["doc"]:  # Only append if there are fields to update
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
