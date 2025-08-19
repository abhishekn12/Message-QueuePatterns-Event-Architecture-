# Event Sourcing for Order Processing in Python
# -----------------------------------------------------------
# Components:
# 1. Event schema
# 2. Aggregate Root (Order)
# 3. Event Store (append-only)
# 4. Projection builders for read models

from dataclasses import dataclass, field
from typing import List, Dict, Any, Callable
import uuid
import datetime

# ---------------- Event Schema -----------------
@dataclass
class Event:
    id: str
    type: str
    aggregate_id: str
    data: Dict[str, Any]
    timestamp: str

    @staticmethod
    def create(event_type: str, aggregate_id: str, data: Dict[str, Any]):
        return Event(
            id=str(uuid.uuid4()),
            type=event_type,
            aggregate_id=aggregate_id,
            data=data,
            timestamp=datetime.datetime.utcnow().isoformat()
        )

# ---------------- Aggregate Root -----------------
@dataclass
class Order:
    id: str
    status: str = "CREATED"
    items: List[Dict[str, Any]] = field(default_factory=list)

    def apply(self, event: Event):
        if event.type == "OrderCreated":
            self.status = "CREATED"
        elif event.type == "ItemAdded":
            self.items.append(event.data)
        elif event.type == "OrderConfirmed":
            self.status = "CONFIRMED"

    def create(cls, order_id: str):
        return Event.create("OrderCreated", order_id, {"order_id": order_id})

    def add_item(self, product_id: str, quantity: int):
        return Event.create("ItemAdded", self.id, {"product_id": product_id, "quantity": quantity})

    def confirm(self):
        return Event.create("OrderConfirmed", self.id, {})

# ---------------- Event Store -----------------
class EventStore:
    def __init__(self):
        self.events: List[Event] = []
        self.subscribers: List[Callable[[Event], None]] = []

    def append(self, event: Event):
        self.events.append(event)
        for sub in self.subscribers:
            sub(event)

    def get_by_aggregate(self, aggregate_id: str) -> List[Event]:
        return [e for e in self.events if e.aggregate_id == aggregate_id]

    def subscribe(self, handler: Callable[[Event], None]):
        self.subscribers.append(handler)

# ---------------- Projection Builders -----------------
class OrderProjection:
    def __init__(self):
        self.orders: Dict[str, Dict[str, Any]] = {}

    def handle(self, event: Event):
        if event.type == "OrderCreated":
            self.orders[event.aggregate_id] = {"status": "CREATED", "items": []}
        elif event.type == "ItemAdded":
            self.orders[event.aggregate_id]["items"].append(event.data)
        elif event.type == "OrderConfirmed":
            self.orders[event.aggregate_id]["status"] = "CONFIRMED"

# ---------------- Example Usage -----------------
store = EventStore()
projection = OrderProjection()
store.subscribe(projection.handle)

# Create an order
order_id = str(uuid.uuid4())
create_event = Event.create("OrderCreated", order_id, {"order_id": order_id})
store.append(create_event)

# Add items
item_event = Event.create("ItemAdded", order_id, {"product_id": "P123", "quantity": 2})
store.append(item_event)

# Confirm order
confirm_event = Event.create("OrderConfirmed", order_id, {})
store.append(confirm_event)

print(projection.orders)
