import motor.motor_asyncio
from core.utils.settings import settings


# Initialize database
client = motor.motor_asyncio.AsyncIOMotorClient(settings.api_db_url)

# Create database
account_db = client.account_db

# Create accounts collection
account_col = account_db.accounts


