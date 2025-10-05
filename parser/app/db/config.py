from sqlalchemy import create_engine
import os

# из докера идут
db_user = os.environ["DB_USER"]
db_password = os.environ["DB_PASSWORD"]
db_name = os.environ["DB_NAME"]
db_port = os.environ["DB_PORT"]


database_url = f"postgresql://{db_user}:{db_password}@kaspi-parser-db:{db_port}/{db_name}"

engine = create_engine(database_url)