import os
import requests


from datetime import date, datetime
from loguru import logger
from peewee import *


from dotenv import load_dotenv

load_dotenv()

dbName = os.getenv("DBNAME", "database")
hostname = os.getenv("HOST", "localhost")
port = int(os.getenv("PORT", 3336))
user = os.getenv("USER")
password = os.getenv("PASSWORD")
apikey = os.getenv("APIKEY")

fxUrl = f"https://v6.exchangerate-api.com/v6/{apikey}/latest/EUR"

fxResponse = requests.get(fxUrl)
fxData = fxResponse.json()

update_date = datetime.utcfromtimestamp(fxData["time_last_update_unix"]).date()

try:
    database = MySQLDatabase(
        dbName,
        thread_safe=True,
        autorollback=True,
        field_types=None,
        operations=None,
        autocommit=False,
        autoconnect=True,
        **{
            "charset": "utf8",
            "sql_mode": "PIPES_AS_CONCAT",
            "use_unicode": True,
            "host": hostname,
            "port": port,
            "user": user,
            "password": password,
        },
    )
except:
    pass


class UnknownField(object):
    def __init__(self, *_, **__):
        pass


class BaseModel(Model):
    class Meta:
        database = database


class Rate(BaseModel):
    date_ = DateField()
    currency = FixedCharField(max_length=3)
    to_eur = FloatField()

    class Meta:
        table_name = "rates"
        indexes = ((("date_", "currency"), True),)


if database.is_closed():
    database.connect()

updateSuccess = 0
updateError = 0

for i in fxData["conversion_rates"]:
    try:
        with database.atomic():
            new_rate = Rate(
                date_=update_date, currency=i, to_eur=fxData["conversion_rates"][i]
            )
            new_rate.save()
            updateSuccess += 1
    except IntegrityError:
        updateError += 1
        logger.debug(f"Error on {i}")
        continue

database.close()
print(
    f"\nfxRates updated {date.today()} with {updateSuccess} successes and {updateError} errors\n"
)
