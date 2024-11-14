# %%

# imports and constants

from typing import Any, Union
import polars as pl
from pydantic import BaseModel
import requests
from datetime import date
from loguru import logger
import json

CATEGORY_TO_SALES_REP: dict[str, str]
CATEGORY_TO_SALES_REP_FILE_NAME: str = 'category_to_sales_rep.json'
with open(CATEGORY_TO_SALES_REP_FILE_NAME) as f:
    CATEGORY_TO_SALES_REP = json.load(f)
    logger.info(
        f'Loaded {CATEGORY_TO_SALES_REP_FILE_NAME} into CATEGORY_TO_SALES_REP constant')

# %%

# WRITE API WRAPPER
# DOCS about the API you can find under https://valueworks-case-study-service-ggaxa6fhg7apbsd6.germanywestcentral-01.azurewebsites.net/docs


class APIWrapper:
    API_URL: str = 'https://valueworks-case-study-service-ggaxa6fhg7apbsd6.germanywestcentral-01.azurewebsites.net'
    OBJECTS: dict[str, Any] = {
        'product': {
            'api_name': 'products',
            'columns': ['product_id', 'product_name', 'category', 'price']
        },
        'order': {
            'api_name': 'orders',
            'columns': ['order_id', 'customer_id', 'order_date']
        },
        'order_item': {
            'api_name': 'order_items',
            'columns': ['order_item_id', 'order_id', 'product_id', 'quantity']
        }
    }

    def __init__(self, username: str, password: str) -> None:
        self.session = requests.Session()
        self.session.auth = (username, password)

    def get_all_rows_of_object(self, object: str) -> list[dict[str, Any]]:
        # TODO: write the logic to retrieve all rows for a single object using the paginated endpoints

        length_data = "Should be a number: the number of rows for the object queried"
        total = "Should be a number: the number of rows for the object reported by the API"

        # this check should make sure all the data is returned
        if length_data != total:
            logger.error(
                f'Data length of retrieved data is not equal to total for {object}')
            return []
        logger.success(f'Successfully retrieved all data for {object}')

        # return the rows as a list of dictionaries
        # return out

# %%

# QUERY THE DATA


USERNAME: str = "I WILL GIVE YOU THE USERNAME"
PASSWORD: str = "I WILL GIVE YOU THE PASSWORD"

api = APIWrapper(username=USERNAME, password=PASSWORD)

# for each object in api.OBJECTS call the get_all_rows_of_object and store it in a dictionary called data
# data will look like this then:
# data = {
#     'product': [...the rows of products],
#     'order': [...the rows of orders],
#     'order_item': [...the rows of order_items],
# }

data: dict[str, list[dict[str, Any]]] = {}

# %%

# TRANSFORM THE DATA

# THIS IS THE DESIRED SCHEMA


class TransformedOrderItemsObject(BaseModel):
    # month = order_date normalized to first day of month
    month: date
    order_item_id: int
    order_id: int
    customer_id: int
    order_date: date
    product_id: int
    product_name: str
    # product_category = category from product renamed to product_category
    product_category: str
    # unit_price = price from product renamed to unit_price
    unit_price: float
    quantity: int
    # item_total = quantity * price from product
    item_total: float
    # sales_rep = for that there is the static mapping from product_category to sales rep specified in CATEGORY_TO_SALES_REP
    sales_rep: str


# load the polars dataframes
# TODO: uncomment the following three lines
# df_p = pl.DataFrame(data['product'])
# df_o = pl.DataFrame(data['order'])
# df_oi = pl.DataFrame(data['order_item'])


# TODO: Perform the join and transformation of existing fields
transformed_df = 'TODO'

# %%

# TODO: Add the column sales_rep info to df using CATEGORY_TO_SALES_REP which is product category to sales rep mapping

transformed_df = "TODO"

# %%

# THIS IS JUST A CHECK TO MAKE SURE THE DATA IS TRANSFORMED CORRECTLY
# TODO: uncomment the code and execute it
# transformed_data = transformed_df.to_dicts()


# Create a list of TransformedOrderItemsObject instances to make sure schema is correct
# it will fail in case the schema is incorrect
# TODO: uncomment the code and execute it

# transformed_objects = [TransformedOrderItemsObject(
#     **item) for item in transformed_data]

# %%

# TODO: Perform a monthly sales analysis by sales rep
# This means you need to group the data by sales rep and month and then perform the aggregations
# for total sales, total quantity, average unit price and product count (these are also the desired column names)

# DESIRED OUTPUT:
# ┌───────────┬────────────┬─────────────┬────────────────┬────────────────────┬───────────────┐
# │ sales_rep ┆ month      ┆ total_sales ┆ total_quantity ┆ average_unit_price ┆ product_count │
# │ ---       ┆ ---        ┆ ---         ┆ ---            ┆ ---                ┆ ---           │
# │ str       ┆ date       ┆ f64         ┆ i64            ┆ f64                ┆ u32           │
# ╞═══════════╪════════════╪═════════════╪════════════════╪════════════════════╪═══════════════╡
# │ Alice     ┆ 2024-10-01 ┆ 517354.41   ┆ 984            ┆ 528.13506          ┆ 265           │
# │ Bob       ┆ 2024-10-01 ┆ 373819.7    ┆ 790            ┆ 477.659443         ┆ 221           │
# │ Eva       ┆ 2024-10-01 ┆ 311498.11   ┆ 611            ┆ 504.734967         ┆ 167           │
# │ Charlie   ┆ 2024-10-01 ┆ 104590.08   ┆ 210            ┆ 489.898624         ┆ 61            │
# │ Alice     ┆ 2024-09-01 ┆ 538009.1    ┆ 967            ┆ 545.748062         ┆ 288           │
# │ …         ┆ …          ┆ …           ┆ …              ┆ …                  ┆ …             │
# │ Charlie   ┆ 2023-02-01 ┆ 112477.15   ┆ 203            ┆ 551.660882         ┆ 59            │
# │ Alice     ┆ 2023-01-01 ┆ 636640.74   ┆ 1159           ┆ 548.65312          ┆ 309           │
# │ Bob       ┆ 2023-01-01 ┆ 386463.67   ┆ 806            ┆ 486.030272         ┆ 226           │
# │ Eva       ┆ 2023-01-01 ┆ 318882.13   ┆ 621            ┆ 515.991889         ┆ 158           │
# │ Charlie   ┆ 2023-01-01 ┆ 104136.19   ┆ 192            ┆ 546.961702         ┆ 53            │
# └───────────┴────────────┴─────────────┴────────────────┴────────────────────┴───────────────┘

# TODO: uncomment the code and execute it
# ctx = pl.SQLContext(transformed_order_items=transformed_df, eager=True)

# USING SQL
# NOTE: table is called transformed_order_items
sql_query = """
    TODO: add sql query here
"""

# TODO: uncomment the code and execute it
# print(ctx.execute(sql_query))

# %%

# Now perform the aggregation using Polars methods to achieve the same result
result_df = "TODO"

# TODO: uncomment the code and execute it
# print(result_df)
# %%
