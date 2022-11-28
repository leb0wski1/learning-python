import asyncpg
from aiohttp import web
from aiohttp.web_app import Application
from aiohttp.web_request import Request
from aiohttp.web_response import Response
from asyncpg import Record
from asyncpg.pool import Pool
from typing import List, Dict

routes = web.RouteTableDef()
DB_KEY = 'database'

async def create_database_pool(app: Application):
    """Создание пула подключений к бд."""
    pool: Pool = await asyncpg.create_pool(host='127.0.0.1',
    port=49154,
    user='postgres',
    password='postgrespw',
    database='postgres',
    min_size=6,
    max_size=6)
    app[DB_KEY] = pool

async def destroy_database_pool(app: Application):
    """Уничтожение пула подключений к бд."""
    pool: Pool = app[DB_KEY]
    await pool.close()

async def db_init(app: Application):
    """Начальная настройка БД, добавление данных в таблицы"""
    connection: Pool = app[DB_KEY]
    _query = ''' 
        CREATE TABLE IF NOT EXISTS Store(
                id SERIAL,
                address varchar,
                PRIMARY KEY(id)
                );
        CREATE TABLE IF NOT EXISTS Item(
                id SERIAL,
                name varchar UNIQUE,
                price float,
                PRIMARY KEY(id)
                );
        CREATE TABLE IF NOT EXISTS Sales(
                id SERIAL,
                sales_time date NOT NULL DEFAULT 'now()',
                item_id INT,
                store_id INT,
                PRIMARY KEY(id),
                FOREIGN KEY(store_id) REFERENCES Store(id),
                FOREIGN KEY(item_id) REFERENCES Item(id)
                );
        INSERT INTO Item(name, price) VALUES 
	            ('banana', 10),
	            ('mango', 30),
	            ('watermelon', 15),
	            ('apple', 5),
	            ('pineapple', 20),
	            ('lemon', 10)
                ON CONFLICT DO NOTHING;
        INSERT INTO Store(address) VALUES
	            ('Magazine1'),
	            ('Magazine2'),
	            ('Magazine3'),
	            ('Magazine4'),
	            ('Magazine5'),
	            ('Magazine6'),
	            ('Magazine7'),
	            ('Magazine8'),
	            ('Magazine9'),
	            ('Magazine10')
                ON CONFLICT DO NOTHING;'''
    await connection.execute(_query)

@routes.get('/item')
async def get_items(request: Request) -> Response:
    """Получение всех товарных позиций"""
    connection: Pool = request.app[DB_KEY]
    _query = 'SELECT id, name, price FROM Item;'
    results: List[Record] = await connection.fetch(_query)
    result_as_dict: List[Dict] = [dict(item) for item in results]
    return web.json_response(result_as_dict)

@routes.get('/store')
async def get_items(request: Request) -> Response:
    """Получение всех магазинов"""
    connection: Pool = request.app[DB_KEY]
    _query = 'SELECT id, address FROM Store;'
    results: List[Record] = await connection.fetch(_query)
    result_as_dict: List[Dict] = [dict(item) for item in results]
    return web.json_response(result_as_dict)

@routes.post('/sales')
async def post_items(request: Request):
    """Запрос POST, принимает json-тело для сохранения данных о произведенной продаже
       Формат json-тела
       {
            "item_id": 4, //ID товара
            "store_id": 2 //ID магазина
        }
    """
    connection: Pool = request.app[DB_KEY]
    request_data = await request.json()
    item_id = request_data['item_id']
    store_id = request_data['store_id']
    _query = 'INSERT INTO sales(item_id, store_id) VALUES ($1, $2)'
    return web.json_response(await connection.execute(_query, item_id, store_id))

@routes.get('/top_sales_store')
async def get_items(request: Request) -> Response:
    """Получение данных по топ 10 самых доходных магазинов за месяц"""
    connection: Pool = request.app[DB_KEY]
    _query = """
        SELECT
	        Store.id,
	        Store.address,
	        sum(Item.price)
	        FROM Sales
	        JOIN Store ON Store.id = Sales.store_id
	        JOIN Item ON Item.id = Sales.item_id
            WHERE sales_time > CURRENT_DATE-30
	        GROUP BY Store.id, Store.address
	        ORDER BY sum DESC
	        LIMIT 10;
    """
    results: List[Record] = await connection.fetch(_query)
    result_as_dict: List[Dict] = [dict(item) for item in results]
    return web.json_response(result_as_dict)

@routes.get('/top_sales_item')
async def get_items(request: Request) -> Response:
    """Получение данных по топ 10 самых продаваемых товаров"""
    connection: Pool = request.app[DB_KEY]
    _query = """
        SELECT
	        Item.id,
	        Item.name,
	        count(Item.price)
	        FROM Sales
	        JOIN Store ON Store.id = Sales.store_id
	        JOIN Item ON Item.id = Sales.item_id
	        GROUP BY Item.id, Item.name
	        ORDER BY count DESC
	        LIMIT 10;
    """
    results: List[Record] = await connection.fetch(_query)
    result_as_dict: List[Dict] = [dict(item) for item in results]
    return web.json_response(result_as_dict)

app = web.Application()
app.on_startup.append(create_database_pool)
app.on_cleanup.append(destroy_database_pool)
app.on_startup.append(db_init)
app.add_routes(routes)
web.run_app(app)