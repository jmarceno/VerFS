import pymongo


class MongoPersitence(object):
    def __init__(self, persistence_type="controllerstatus"):
        if type(persistence_type) != str:
            print("persistence_type needs to be a string informing the type of information you want yo store. Falling back to 'controllerstatus'")
            persistence_type = "controllerstatus"

        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.database = self.client[persistence_type]
        self.collection = None

        dblist = self.client.list_database_names()
        if persistence_type in dblist:
            if persistence_type == "controllerstatus":
                collection_list = self.database.list_collection_names()
                if "status" not in collection_list:
                    self.collection = self.database["status"]
                    self.collection.insert_one({'1': 1})
                else:
                    self.collection = self.database["status"]
        else:
            print("No persistence type for " + persistence_type)
            exit(1)

    def store_data(self, data):
        # Example -> mongo.store_data({'controladorXPTO':util.localtime.localtime(),"status":'$JSM,1,R,AAAA,10,10/0F/0F/0F/0F/0F/0F/0F/0F/0F/0F/0F/0F/0F/0F/0F'})
        if type(data) is dict:
            self.collection.insert_one(data)
            return 1
        else:
            print("Data should be passed as a dict to be stored.")
            return 0

