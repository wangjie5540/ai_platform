import asyncio

import motor.motor_asyncio as motor_asyncio

from digitforce.aip.common.config.bigdata_config import MONGODB_HOST, MONGODB_PORT, MONGODB_USER, MONGODB_PASSWORD


class MongoDBClient:

    def __init__(self, host, port, user, password):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.client = None
        self.db = None
        self.collection = None
        self.loop = asyncio.get_event_loop()

    @property
    def get_conn_str(self):
        return f'mongodb://{self.user}:{self.password}@{self.host}:{self.port}/'

    def get_client(self):
        if not self.client:
            self.client = motor_asyncio.AsyncIOMotorClient(self.get_conn_str)
        return self.client

    def set_db(self, db):
        self.db = self.get_client()[db]
        return self.db

    def set_collection(self, collection):
        self.collection = self.db[collection]

    def find_one(self, cond: dict):
        async def _find_one():
            result = await self.collection.find_one(cond)
            return result

        return self.loop.run_until_complete(_find_one())

    def find(self, cond: dict, length=5000):
        async def _find():
            result = self.collection.find(cond)
            return await result.to_list(length)

        return self.loop.run_until_complete(_find())

    def insert_one(self, doc: dict):
        async def _insert_one():
            result = await self.collection.insert_one(doc)
            return result

        return self.loop.run_until_complete(_insert_one())

    def insert_many(self, doc_list: list):
        async def _insert_one():
            await self.collection.insert_many(doc_list)

        self.loop.run_until_complete(_insert_one())

    def count(self, cond={}):
        async def _count():
            n = await self.collection.count_documents(cond)
            return n

        return self.loop.run_until_complete(_count())

    def replace_one(self, filter_cond: dict, update_doc: dict, upsert: bool = True):
        async def _replace_one():
            await self.collection.replace_one(filter=filter_cond, replacement=update_doc, upsert=upsert)

        self.loop.run_until_complete(_replace_one())

    def close(self):
        self.client.close()


mongodb_client_cli = MongoDBClient(MONGODB_HOST, MONGODB_PORT, MONGODB_USER, MONGODB_PASSWORD)

if __name__ == '__main__':
    client = MongoDBClient(host='172.21.32.143', port=27017, user='admin', password='123456')
    client.get_client()
    client.set_db('recommend')

    client.set_collection('rank_goods_features')

    # print(client.find({'sku': {'$exists': True}}))
    # print(client.count())
    client.replace_one({'sku': 'P196935'}, {'sku': 'P196935', 'cate': '休闲小食', 'brand': '妙香山'})
    # client.replace_one({'sku': 'P196935'}, {'sku': 'P196935', 'cate': '休闲da食'})
    print(client.find_one({'sku': 'P196935'}))
