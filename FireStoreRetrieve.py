import os
from google.cloud import firestore

# Set the environment variable for Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/manish/airasia-hotelsbe-prd.json"

# Initialize Firestore client
db = firestore.Client()


def get_documents_from_collection(collection_name):
    # Reference to the collection
    collection_ref = db.collection(collection_name)

    query = collection_ref.where('aaHotelId', '==', 'b2e9').limit(1)
    # Fetch documents
    docs = query.stream()

    for doc in docs:
        print(f'Document ID: {doc.id}')
        doc_dict = doc.to_dict()

        # Accessing nested fields like address.city and address.country
        if 'address' in doc_dict:
            address = doc_dict['address']
            if 'city' in address:
                print(f'Document City: {address["city"]}')
            if 'country' in address:
                print(f'Document Country: {address["country"]}')

        # Alternatively, using try-except for safer access
        try:
            print(f'Document City: {doc_dict["address"]["city"]}')
            print(f'Document Country: {doc_dict["address"]["country"]}')
        except KeyError:
            print('Address details not found in the document.')

        print(f'Document Data: {doc_dict}')


if __name__ == "__main__":
    collection_name = 'hotel-content-en-us'
    get_documents_from_collection(collection_name)
