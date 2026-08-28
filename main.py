from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
import pika
import json

app = FastAPI(title="Order Service")

# Database Connection (Creates database order_db and collection orders automatically)
client = MongoClient("mongodb://mongodb:27017/")
db = client["order_db"]
orders_collection = db["orders"]

class OrderCreate(BaseModel):
    user_id: str
    product_id: str

def publish_event(event_data):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('mongodb'))
        channel = connection.channel()
        channel.queue_declare(queue='ORDER_CREATED')
        channel.basic_publish(exchange='', routing_key='ORDER_CREATED', body=json.dumps(event_data))
        connection.close()
    except Exception as e:
        print(f"RabbitMQ Publish Failed: {e}")

@app.post("/orders", status_code=201)
def create_order(order: OrderCreate):
    order_dict = order.model_dump()
    order_dict["status"] = "CREATED"
    
    res = orders_collection.insert_one(order_dict)
    order_id = str(res.inserted_id)
    order_dict["_id"] = order_id
    
    # Decoupled Event-Driven Messaging
    publish_event({"order_id": order_id, "user_id": order.user_id, "product_id": order.product_id})
    
    return order_dict
