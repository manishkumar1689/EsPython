import os
from google.cloud import firestore
from openpyxl import Workbook

# Initialize workbook and sheet
workbook = Workbook()
sheet = workbook.active

# Set headers for the Excel sheet
sheet.append(['aaId', 'city', 'country'])

# Set the environment variable for Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/manish/airasia-hotelsbe-prd.json"

# Initialize Firestore client
db = firestore.Client()

output = []
seen_aaid = set()

def get_documents_in_batches(collection_name, batch_size=1000):
    # Reference to the collection
    collection_ref = db.collection(collection_name)

    # Initialize batch cursor
    batch_cursor = None

    try:
        while True:
            # Define query with limit and start_after for pagination
            if batch_cursor:
                query = collection_ref.order_by('__name__').start_after({u'__name__': batch_cursor}).limit(batch_size)
            else:
                query = collection_ref.order_by('__name__').limit(batch_size)

            # Retrieve batch of documents
            results = query.stream()
            batch_docs = []

            for doc in results:
                # Accessing nested fields like address.city and address.country
                doc_dict = doc.to_dict()
                address = doc_dict.get('address', {})
                city = address.get('city', 'N/A')
                country = address.get('country', 'N/A')
                aaId = doc_dict.get('aaHotelId', 'N/A')

                # Ensure aaId is unique in output
                if aaId not in seen_aaid:
                    output.append([aaId, city, country])
                    seen_aaid.add(aaId)

                # Print document details
                print(f'Document City: {city}')
                print(f'Document Country: {country}')
                print(f'Document ID: {doc.id}')
                print(f'Document Data: {doc_dict}')
                print('---------------------------------')

                batch_docs.append(doc)
                batch_cursor = doc.id

            # Stop if no more documents or output exceeds batch size
            if len(batch_docs) < batch_size:
                break

        # Write data to Excel file
        for row in output:
            sheet.append(row)

        workbook.save(filename="HotelContent.xlsx")

        print("Data retrieval and export completed successfully.")

    except Exception as e:
        print(f"Error retrieving data: {e}")

if __name__ == "__main__":
    collection_name = 'hotel-content-en-us'
    get_documents_in_batches(collection_name)
