import os
import requests
import sys

from datetime import date, datetime
from peewee import *
from typing import Dict

from dotenv import load_dotenv

load_dotenv()

from loguru import logger

logger.remove()
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD at HH:mm:ss} | <lvl>{level}</lvl> | {message}",
)


try:
    database = MySQLDatabase(
        os.getenv("DBNAME", "database"),
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
            "host": os.getenv("HOST", "localhost"),
            "port": int(os.getenv("PORT", 3336)),
            "user": os.getenv("USER"),
            "password": os.getenv("PASSWORD"),
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


def getRates() -> Dict:
    apikey = os.getenv("APIKEY")
    fxUrl = f"https://v6.exchangerate-api.com/v6/{apikey}/latest/EUR"
    fxResponse = requests.get(fxUrl)
    fxData = fxResponse.json()
    return fxData


def updateDB(database: MySQLDatabase) -> None:
    if database.is_closed():
        database.connect()

    if date.today() == checkLatest(database):
        logger.error(f"already updated for {date.today()}")
        return

    fxData = getRates()
    update_date = datetime.utcfromtimestamp(fxData["time_last_update_unix"]).date()

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
            logger.opt(colors=True).debug(f"<red>Error for {i}</red>")
            continue

    database.close()

    logger.info(
        f"{date.today()}: fxRates processed {updateError + updateSuccess} total. "
    )

    if updateError == 0:
        logger.opt(colors=True).success(
            f"<green>{updateSuccess} rate{'s' if updateSuccess != 1 else ''} updated.</>"
        )
    else:
        logger.opt(colors=True).error(
            f"<red>{updateError} error{'s' if updateError != 1 else ''} during processing.</>"
        )
    return


def checkLatest(database: MySQLDatabase) -> date:
    query = Rate.select(fn.MAX(Rate.date_))
    return query.scalar()


updateDB(database)
