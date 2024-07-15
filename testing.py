
# import time
# import logging
# from elasticsearch import Elasticsearch, helpers
# from openpyxl import Workbook
# import pandas as pd
#
# file_path = 'output.xlsx'
# df = pd.read_excel(file_path, header=None, engine='openpyxl')
#
# # Convert DataFrame to dictionary (HashMap)
# data_dict = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
#
# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
#
# # Initialize Elasticsearch client
# USERNAME = 'elastic'
# PASSWORD = 'IV9PCi6eyzE7W9PHy5gF'
#
# # Elasticsearch connection setup with Basic Auth
# es = Elasticsearch(['http://35.224.91.31:9200'],
#                    http_auth=(USERNAME, PASSWORD),
#                    scheme='http',
#                    port=9200,
#                    verify_certs=False)
#
# index_a = "hotel-suggest-v8"
# batch_size = 5000  # Adjust batch size as needed for your system's capacity
# current_from = 0
# total_docs = 0
#
# # index_a_query = {
# #         # "query": {
# #         #     "term": {"field_A": field_b_value}
# #         # }
# #         "query": {
# #             "match_all": {"type":"hotel"}  # Retrieve all documents in the index
# #         },
# #         "size": batch_size
# #     }
# index_a_query = {
#     "query": {
#         "bool": {
#             "must": [
#                 {"match_all": {}}
#             ],
#             "filter": [
#                 {"term": {"type": "hotel"}}
#             ]
#         }
#     },
#     "size": batch_size
# }
# # Retrieve batch of documents from Elasticsearch
# # if 3000012573 in data_dict:
# #     print(f"hello manish {type(3000012573)}")
# # print(f'Dictionary type: {data_dict.get("1951","")}')
# index_a_results = es.search(index=index_a, body=index_a_query, scroll='5m')
# scroll_id=index_a_results["_scroll_id"]
#
# # Step 2 & 3: Compare and Prepare Updates for Index A
# updates = []
# # Function to perform bulk updates
# def bulk_update(updates, retries=3):
#     attempt = 0
#     while attempt < retries:
#         try:
#             helpers.bulk(es, updates)
#             return True
#         except Exception as e:
#             logger.error(f"Bulk update attempt {attempt + 1} failed: {e}")
#             time.sleep(2 ** attempt)  # Exponential backoff
#             attempt += 1
#     return False
#
# # Main loop for scrolling and updating documents
# scroll_id = None  # Initialize scroll_id
# batch_size = 1000  # Adjust as needed
#
# while True:
#     try:
#         # Perform the initial search or continue scrolling
#         if scroll_id is None:
#             index_a_query = {
#                 "query": {
#                     "bool": {
#                         "must": [
#                             {"match_all": {}}
#                         ],
#                         "filter": [
#                             {"term": {"type": "hotel"}}
#                         ]
#                     }
#                 },
#                 "size": batch_size
#             }
#             response = es.search(index=index_a, body=index_a_query, scroll='5m')
#             scroll_id = response["_scroll_id"]
#             index_a_results = response["hits"]["hits"]
#         else:
#             response = es.scroll(scroll_id=scroll_id, scroll='5m')
#             index_a_results = response["hits"]["hits"]
#
#         if not index_a_results:
#             logger.info("No more hits returned by scroll.")
#             break
#
#         # Process documents and prepare updates
#         updates = []
#         for doc_a in index_a_results:
#             try:
#                 additional_info = data_dict.get(int(doc_a["_source"]["id"]), "")
#
#                 update_doc = {
#                     "_op_type": "update",
#                     "_index": index_a,
#                     "_id": doc_a["_id"],
#                     "doc": {}
#                 }
#
#                 # Define fields to check and update
#                 fields_to_update = [
#                     "keyword_full_name_en-us",
#                     "keyword_full_name_zh-cn",
#                     "keyword_full_name_zh-tw",
#                     "keyword_full_name_ja-jp",
#                     "keyword_full_name_ko-kr",
#                     "keyword_full_name_th-th",
#                     "keyword_full_name_id-id"
#                 ]
#
#                 # Iterate over fields and add to update_doc if they have a value
#                 for field in fields_to_update:
#                     value = doc_a["_source"].get(field)
#                     if value:  # Check if the value is not None or empty
#                         update_doc["doc"][field] = f"{value}, {additional_info}"
#
#                 if update_doc["doc"]:  # Only append if there are fields to update
#                     logger.debug(f"Prepared update: {update_doc}")
#                     updates.append(update_doc)
#
#             except KeyError as ke:
#                 logger.warning(f"KeyError: Missing key in document {doc_a['_id']}: {ke}")
#             except ValueError as ve:
#                 logger.warning(f"ValueError: Unable to convert ID to integer for document {doc_a['_id']}: {ve}")
#             except Exception as e:
#                 logger.error(f"Error processing document {doc_a['_id']}: {e}")
#
#         # Execute bulk update
#         if updates:
#             success = bulk_update(updates)
#             if not success:
#                 logger.error("Failed to perform bulk update after retries.")
#                 break
#
#     except Exception as e:
#         logger.error(f"Error during scrolling or updating: {e}")
#         break
#
# logger.info("Processing complete.")

# from elasticsearch import Elasticsearch, helpers
# from openpyxl import Workbook
#
# workbook = Workbook()
# sheet = workbook.active
#
# # Elasticsearch Basic Auth credentials
# USERNAME = 'elastic'
# PASSWORD = 'IV9PCi6eyzE7W9PHy5gF'
#
# # Elasticsearch connection setup with Basic Auth
# es = Elasticsearch(['http://35.224.91.31:9200'],
#                    http_auth=(USERNAME, PASSWORD),
#                    scheme='http',
#                    port=9200,
#                    verify_certs=False)
#
# index = "smartfill-location-suggest-v1"
# key1 = "id"
# key2 = "name_en-us"
# key3 = "name_zh-cn"
# key4 = "name_zh-tw"
# key5 = "name_ja-jp"
# key6 = "name_ko-kr"
# key7 = "name_th-th"
# key8 = "name_id-id"
#
# # Define batch size and initial parameters
# batch_size = 10000  # Adjust batch size as needed for your system's capacity
# current_from = 0
# total_docs = 0
#
# output = {}
# data = []
# query = {
#     "query": {
#         "match_all": {}  # Retrieve all documents in the index
#     },
#     # "_source": [key1, key2], # Include only the specified key in the returned documents
#     # "from": current_from,
#     # "doc_type": "_doc",
#     # "size": batch_size
# }
#
# results = es.search(index=index, body=query, scroll='5m', size=batch_size)
# # print("Type of initial_search:", results[0])
#
# if isinstance(results, dict):
#     # Check if the _scroll_id is in the response
#     if '_scroll_id' in results:
#         scroll_id = results['_scroll_id']
#         print("Scroll ID obtained:", scroll_id)
#     else:
#         print("No _scroll_id found in the initial search response.")
# else:
#     print("Unexpected response format, expected a dictionary.")
#
# # Loop to retrieve all documents
# while True:
#     try:
#         # Retrieve batch of documents from Elasticsearch
#         results_data = es.scroll(scroll_id=scroll_id, scroll='1m')["hits"]["hits"]
#
#         batch_docs = len(results_data)
#         print(f"Type of: {type(results_data)}")
#         print(f"Total documents retrieved: {batch_docs}")
#
#         if not results_data:
#             print("No more hits returned by scroll.")
#             break
#
#         # Process documents and extract the value for the specified key
#         for doc in results_data:
#             if key1 in doc["_source"]:
#                 value = doc["_source"][key1]
#                 output[doc["_source"][key1]] = doc["_source"][key2]
#                 data.append([doc["_source"][key1], doc["_source"][key2], doc["_source"][key3], doc["_source"][key4], doc["_source"][key5], doc["_source"][key6], doc["_source"][key7], doc["_source"][key8]])
#                 # Print or process the value here as needed
#                 print(value)
#
#     except Exception as e:
#         print({e})
#         break
#
# # Print total documents retrieved
# print(f"Total documents retrieved: {len(data)}")
#
# # Save data to Excel file
# for row in data:
#     sheet.append(row)
# workbook.save(filename="output1.xlsx")

from elasticsearch import Elasticsearch
from openpyxl import Workbook

# Initialize workbook and sheet
workbook = Workbook()
sheet = workbook.active

# Elasticsearch Basic Auth credentials
USERNAME = 'elastic'
PASSWORD = 'IV9PCi6eyzE7W9PHy5gF'

# Elasticsearch connection setup with Basic Auth
es = Elasticsearch(['http://35.224.91.31:9200'],
                   http_auth=(USERNAME, PASSWORD),
                   scheme='http',
                   port=9200,
                   verify_certs=False)

# Define index and keys
index = "smartfill-location-suggest-v1"
keys = ["id", "name_en-us", "name_zh-cn", "name_zh-tw",
        "name_ja-jp", "name_ko-kr", "name_th-th", "name_id-id"]

# Define batch size and initial parameters
batch_size = 10000  # Adjust batch size as needed for your system's capacity
output = []
total_docs = 0

# Define query to retrieve all documents
query = {
    "query": {
        "match_all": {}  # Retrieve all documents in the index
    }
}

# Perform initial search
results = es.search(index=index, body=query, scroll='5m', size=batch_size)

# Check response format
if isinstance(results, dict):
    scroll_id = results.get('_scroll_id', None)
    if scroll_id:
        print("Scroll ID obtained:", scroll_id)
    else:
        print("No _scroll_id found in the initial search response.")
else:
    print("Unexpected response format, expected a dictionary.")

# Loop to retrieve all documents
while True:
    try:
        # Retrieve batch of documents from Elasticsearch using scroll API
        results_data = es.scroll(scroll_id=scroll_id, scroll='1m')["hits"]["hits"]

        # Check if there are no more hits returned by scroll
        if not results_data:
            print("No more hits returned by scroll.")
            break

        # Process documents
        for doc in results_data:
            # Extract values for each key, assigning empty string if key is missing
            doc_values = [doc["_source"].get(key, "") for key in keys]
            output.append(doc_values)

        total_docs += len(results_data)

    except Exception as e:
        print(f"Error retrieving documents: {e}")
        break

# Print total documents retrieved
print(f"Total documents retrieved: {total_docs}")

# Save data to Excel file
for row in output:
    sheet.append(row)

workbook.save(filename="output2.xlsx")




