from pymongo import MongoClient, errors
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus
def get_mongo_data():
    try:
        # MongoDB connection details
        mongo_uri = "mongodb://localhost:27017/"
        database_name = "my_database"
        collection_name = "my_collection"
        username="manishkumarm@airasia.com"
        password="Haryanao@123"
        escaped_username = quote_plus(username)
        escaped_password = quote_plus(password)

        #uri = (f"mongodb+srv://catalog-user:CDdtEQ1JiXpmt9UQ@hotels-mongodb-pri.lbxrj.mongodb.net/catalog_hotel?retryWrites=true&w=majority&readPreference=secondary")
        uri = ""
        print("here I am")
        # Create a new client and connect to the server
        client = MongoClient(uri, server_api=ServerApi('1'))
        print("you are there")
        # Send a ping to confirm a successful connection
        try:
            client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)

        # Connect to MongoDB
        #client = MongoClient(mongo_uri)
        database = client["hotels-mongodb"]
        collection = database["unified-content-en-us"]

        # Query to retrieve all documents
        query = {"_id":"6adc"}

        # Retrieve documents
        documents = collection.find(query)

        # Process and print the documents
        for doc in documents:
            print(doc)

    except errors.ConnectionError as ce:
        print("Could not connect to MongoDB:", ce)
    except errors.PyMongoError as pme:
        print("An error occurred with PyMongo:", pme)
    finally:
        # Close the MongoDB connection
        client.close()

# Run the function to get data from MongoDB
get_mongo_data()
