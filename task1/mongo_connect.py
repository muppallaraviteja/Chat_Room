import pymongo
import constants


def connectMongo():
    url = constants.host
    client = pymongo.MongoClient(url)
    db = client[constants.db]
    collection = db[constants.collection]
    return collection
