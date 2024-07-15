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
                   basic_auth=(USERNAME, PASSWORD),
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

workbook.save(filename="output3.xlsx")




