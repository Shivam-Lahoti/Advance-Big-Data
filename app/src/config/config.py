import redis
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
    GOOGLE_DISCOVERY_URL = os.getenv("GOOGLE_DISCOVERY_URL")

    @staticmethod
    def connect_redis():
        return redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)