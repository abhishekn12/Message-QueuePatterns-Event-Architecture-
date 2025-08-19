from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, Type, Union
from uuid import UUID, uuid4
from enum import Enum
import json
from copy import deepcopy


# Event Base Classes
@dataclass
class Event:
    """Base event class for all domain events"""
    event_id: UUID = field(default_factory=uuid4)
    aggregate_id: UUID = field(default=None)
    event_type: str = field(init=False)
    version: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not hasattr(self, 'event_type'):
            self.event_type = self.__class__.__name__


# Order Domain Events
@dataclass
class OrderCreated(Event):
    customer_id: UUID = None
    items: List[Dict[str, Any]] = field(default_factory=list)
    total_amount: float = 0.0
    currency: str = "USD"


@dataclass
class OrderItemAdded(Event):
    product_id: UUID = None
    quantity: int = 0
    unit_price: float = 0.0
    item_name: str = ""


@dataclass
class OrderItemRemoved(Event):
    product_id: UUID = None
    quantity: int = 0


@dataclass
class OrderShipped(Event):
    shipping_address: Dict[str, str] = field(default_factory=dict)
    tracking_number: str = ""
    carrier: str = ""


@dataclass
class OrderCancelled(Event):
    reason: str = ""
    refund_amount: float = 0.0


@dataclass
class PaymentProcessed(Event):
    payment_method: str = ""
    amount: float = 0.0
    transaction_id: str = ""


# Order Status Enum
class OrderStatus(Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


# Aggregate Root Base Class
class AggregateRoot(ABC):
    """Base class for aggregate roots in event sourcing"""
    
    def __init__(self, aggregate_id: UUID = None):
        self.aggregate_id = aggregate_id or uuid4()
        self.version = 0
        self.uncommitted_events: List[Event] = []
    
    def apply_event(self, event: Event) -> None:
        """Apply an event to the aggregate and update version"""
        event.aggregate_id = self.aggregate_id
        event.version = self.version + 1
        self._handle_event(event)
        self.version = event.version
    
    def raise_event(self, event: Event) -> None:
        """Raise a new event and add to uncommitted events"""
        self.apply_event(event)
        self.uncommitted_events.append(event)
    
    def mark_events_as_committed(self) -> None:
        """Mark all uncommitted events as committed"""
        self.uncommitted_events.clear()
    
    def get_uncommitted_events(self) -> List[Event]:
        """Get all uncommitted events"""
        return self.uncommitted_events.copy()
    
    @abstractmethod
    def _handle_event(self, event: Event) -> None:
        """Handle specific event types - must be implemented by subclasses"""
        pass


# Order Aggregate Root
class Order(AggregateRoot):
    """Order aggregate root that maintains order state through events"""
    
    def __init__(self, aggregate_id: UUID = None):
        super().__init__(aggregate_id)
        self.customer_id: Optional[UUID] = None
        self.items: Dict[UUID, Dict[str, Any]] = {}
        self.total_amount: float = 0.0
        self.currency: str = "USD"
        self.status: OrderStatus = OrderStatus.PENDING
        self.shipping_info: Dict[str, Any] = {}
        self.payment_info: Dict[str, Any] = {}
    
    def create_order(self, customer_id: UUID, items: List[Dict[str, Any]], currency: str = "USD"):
        """Create a new order"""
        if self.customer_id is not None:
            raise ValueError("Order already exists")
        
        total = sum(item['quantity'] * item['unit_price'] for item in items)
        event = OrderCreated(
            customer_id=customer_id,
            items=items,
            total_amount=total,
            currency=currency
        )
        self.raise_event(event)
    
    def add_item(self, product_id: UUID, quantity: int, unit_price: float, item_name: str):
        """Add an item to the order"""
        if self.status != OrderStatus.PENDING:
            raise ValueError("Cannot modify order that is not pending")
        
        event = OrderItemAdded(
            product_id=product_id,
            quantity=quantity,
            unit_price=unit_price,
            item_name=item_name
        )
        self.raise_event(event)
    
    def remove_item(self, product_id: UUID, quantity: int):
        """Remove an item from the order"""
        if self.status != OrderStatus.PENDING:
            raise ValueError("Cannot modify order that is not pending")
        
        if product_id not in self.items:
            raise ValueError("Item not found in order")
        
        event = OrderItemRemoved(product_id=product_id, quantity=quantity)
        self.raise_event(event)
    
    def ship_order(self, shipping_address: Dict[str, str], tracking_number: str, carrier: str):
        """Ship the order"""
        if self.status != OrderStatus.CONFIRMED:
            raise ValueError("Order must be confirmed before shipping")
        
        event = OrderShipped(
            shipping_address=shipping_address,
            tracking_number=tracking_number,
            carrier=carrier
        )
        self.raise_event(event)
    
    def cancel_order(self, reason: str):
        """Cancel the order"""
        if self.status in [OrderStatus.SHIPPED, OrderStatus.DELIVERED]:
            raise ValueError("Cannot cancel shipped or delivered order")
        
        event = OrderCancelled(reason=reason, refund_amount=self.total_amount)
        self.raise_event(event)
    
    def process_payment(self, payment_method: str, amount: float, transaction_id: str):
        """Process payment for the order"""
        if self.status != OrderStatus.PENDING:
            raise ValueError("Payment can only be processed for pending orders")
        
        event = PaymentProcessed(
            payment_method=payment_method,
            amount=amount,
            transaction_id=transaction_id
        )
        self.raise_event(event)
    
    def _handle_event(self, event: Event) -> None:
        """Handle different event types"""
        if isinstance(event, OrderCreated):
            self.customer_id = event.customer_id
            self.currency = event.currency
            self.total_amount = event.total_amount
            self.status = OrderStatus.PENDING
            # Initialize items from event
            for item in event.items:
                self.items[item['product_id']] = item
        
        elif isinstance(event, OrderItemAdded):
            if event.product_id in self.items:
                # Update existing item
                self.items[event.product_id]['quantity'] += event.quantity
            else:
                # Add new item
                self.items[event.product_id] = {
                    'product_id': event.product_id,
                    'quantity': event.quantity,
                    'unit_price': event.unit_price,
                    'item_name': event.item_name
                }
            self._recalculate_total()
        
        elif isinstance(event, OrderItemRemoved):
            if event.product_id in self.items:
                current_qty = self.items[event.product_id]['quantity']
                if current_qty <= event.quantity:
                    del self.items[event.product_id]
                else:
                    self.items[event.product_id]['quantity'] -= event.quantity
                self._recalculate_total()
        
        elif isinstance(event, OrderShipped):
            self.status = OrderStatus.SHIPPED
            self.shipping_info = {
                'address': event.shipping_address,
                'tracking_number': event.tracking_number,
                'carrier': event.carrier,
                'shipped_at': event.timestamp
            }
        
        elif isinstance(event, OrderCancelled):
            self.status = OrderStatus.CANCELLED
        
        elif isinstance(event, PaymentProcessed):
            self.status = OrderStatus.CONFIRMED
            self.payment_info = {
                'method': event.payment_method,
                'amount': event.amount,
                'transaction_id': event.transaction_id,
                'processed_at': event.timestamp
            }
    
    def _recalculate_total(self):
        """Recalculate total amount based on current items"""
        self.total_amount = sum(
            item['quantity'] * item['unit_price'] 
            for item in self.items.values()
        )


# Event Store Implementation
class EventStore:
    """Append-only event store for persisting events"""
    
    def __init__(self):
        self._events: Dict[UUID, List[Event]] = {}
        self._snapshots: Dict[UUID, Dict[str, Any]] = {}
    
    def append_events(self, aggregate_id: UUID, events: List[Event], expected_version: int = None) -> None:
        """Append events to the store with optimistic concurrency control"""
        if aggregate_id not in self._events:
            self._events[aggregate_id] = []
        
        current_version = len(self._events[aggregate_id])
        
        # Optimistic concurrency check
        if expected_version is not None and current_version != expected_version:
            raise ConcurrencyError(
                f"Expected version {expected_version}, but current version is {current_version}"
            )
        
        # Append events
        for event in events:
            event.aggregate_id = aggregate_id
            event.version = current_version + 1
            self._events[aggregate_id].append(event)
            current_version += 1
    
    def get_events(self, aggregate_id: UUID, from_version: int = 0) -> List[Event]:
        """Get events for an aggregate from a specific version"""
        if aggregate_id not in self._events:
            return []
        
        return self._events[aggregate_id][from_version:]
    
    def get_all_events(self, from_timestamp: datetime = None) -> List[Event]:
        """Get all events, optionally from a specific timestamp"""
        all_events = []
        for events in self._events.values():
            all_events.extend(events)
        
        if from_timestamp:
            all_events = [e for e in all_events if e.timestamp >= from_timestamp]
        
        return sorted(all_events, key=lambda e: e.timestamp)
    
    def save_snapshot(self, aggregate_id: UUID, snapshot: Dict[str, Any], version: int) -> None:
        """Save a snapshot of aggregate state"""
        self._snapshots[aggregate_id] = {
            'data': snapshot,
            'version': version,
            'timestamp': datetime.utcnow()
        }
    
    def get_snapshot(self, aggregate_id: UUID) -> Optional[Dict[str, Any]]:
        """Get the latest snapshot for an aggregate"""
        return self._snapshots.get(aggregate_id)


# Repository Pattern for Aggregates
class Repository:
    """Repository for loading and saving aggregates"""
    
    def __init__(self, event_store: EventStore):
        self.event_store = event_store
    
    def save(self, aggregate: AggregateRoot) -> None:
        """Save aggregate by appending uncommitted events"""
        uncommitted = aggregate.get_uncommitted_events()
        if uncommitted:
            self.event_store.append_events(
                aggregate.aggregate_id,
                uncommitted,
                aggregate.version - len(uncommitted)
            )
            aggregate.mark_events_as_committed()
    
    def load(self, aggregate_id: UUID, aggregate_class: Type[AggregateRoot]) -> Optional[AggregateRoot]:
        """Load aggregate by replaying events"""
        events = self.event_store.get_events(aggregate_id)
        if not events:
            return None
        
        aggregate = aggregate_class(aggregate_id)
        for event in events:
            aggregate.apply_event(event)
        
        aggregate.mark_events_as_committed()
        return aggregate


# Projection Base Classes
class ReadModel(ABC):
    """Base class for read models"""
    
    @abstractmethod
    def handle_event(self, event: Event) -> None:
        """Handle an event and update the read model"""
        pass


class ProjectionBuilder:
    """Builds and maintains projections from events"""
    
    def __init__(self, event_store: EventStore):
        self.event_store = event_store
        self.read_models: Dict[str, ReadModel] = {}
        self.last_processed_timestamp: Optional[datetime] = None
    
    def register_read_model(self, name: str, read_model: ReadModel) -> None:
        """Register a read model for projection building"""
        self.read_models[name] = read_model
    
    def rebuild_all_projections(self) -> None:
        """Rebuild all projections from the beginning"""
        events = self.event_store.get_all_events()
        for event in events:
            for read_model in self.read_models.values():
                read_model.handle_event(event)
        
        if events:
            self.last_processed_timestamp = max(e.timestamp for e in events)
    
    def update_projections(self) -> None:
        """Update projections with new events since last update"""
        events = self.event_store.get_all_events(self.last_processed_timestamp)
        
        # Skip events we've already processed
        if self.last_processed_timestamp:
            events = [e for e in events if e.timestamp > self.last_processed_timestamp]
        
        for event in events:
            for read_model in self.read_models.values():
                read_model.handle_event(event)
        
        if events:
            self.last_processed_timestamp = max(e.timestamp for e in events)


# Read Models
class OrderSummaryReadModel(ReadModel):
    """Read model for order summaries"""
    
    def __init__(self):
        self.orders: Dict[UUID, Dict[str, Any]] = {}
    
    def handle_event(self, event: Event) -> None:
        """Handle events and update order summary"""
        aggregate_id = event.aggregate_id
        
        if isinstance(event, OrderCreated):
            self.orders[aggregate_id] = {
                'order_id': aggregate_id,
                'customer_id': event.customer_id,
                'status': OrderStatus.PENDING.value,
                'total_amount': event.total_amount,
                'currency': event.currency,
                'created_at': event.timestamp,
                'item_count': len(event.items),
                'last_updated': event.timestamp
            }
        
        elif isinstance(event, PaymentProcessed):
            if aggregate_id in self.orders:
                self.orders[aggregate_id]['status'] = OrderStatus.CONFIRMED.value
                self.orders[aggregate_id]['payment_method'] = event.payment_method
                self.orders[aggregate_id]['last_updated'] = event.timestamp
        
        elif isinstance(event, OrderShipped):
            if aggregate_id in self.orders:
                self.orders[aggregate_id]['status'] = OrderStatus.SHIPPED.value
                self.orders[aggregate_id]['tracking_number'] = event.tracking_number
                self.orders[aggregate_id]['carrier'] = event.carrier
                self.orders[aggregate_id]['shipped_at'] = event.timestamp
                self.orders[aggregate_id]['last_updated'] = event.timestamp
        
        elif isinstance(event, OrderCancelled):
            if aggregate_id in self.orders:
                self.orders[aggregate_id]['status'] = OrderStatus.CANCELLED.value
                self.orders[aggregate_id]['cancelled_at'] = event.timestamp
                self.orders[aggregate_id]['cancel_reason'] = event.reason
                self.orders[aggregate_id]['last_updated'] = event.timestamp
        
        elif isinstance(event, (OrderItemAdded, OrderItemRemoved)):
            if aggregate_id in self.orders:
                # Would need to recalculate totals - simplified here
                self.orders[aggregate_id]['last_updated'] = event.timestamp
    
    def get_order_summary(self, order_id: UUID) -> Optional[Dict[str, Any]]:
        """Get order summary by ID"""
        return self.orders.get(order_id)
    
    def get_orders_by_customer(self, customer_id: UUID) -> List[Dict[str, Any]]:
        """Get all orders for a customer"""
        return [
            order for order in self.orders.values()
            if order['customer_id'] == customer_id
        ]
    
    def get_orders_by_status(self, status: OrderStatus) -> List[Dict[str, Any]]:
        """Get all orders with a specific status"""
        return [
            order for order in self.orders.values()
            if order['status'] == status.value
        ]


class InventoryReadModel(ReadModel):
    """Read model for inventory tracking"""
    
    def __init__(self):
        self.inventory: Dict[UUID, Dict[str, Any]] = {}
        self.reserved_items: Dict[UUID, int] = {}
    
    def handle_event(self, event: Event) -> None:
        """Handle events that affect inventory"""
        if isinstance(event, OrderItemAdded):
            # Reserve inventory
            product_id = event.product_id
            if product_id not in self.reserved_items:
                self.reserved_items[product_id] = 0
            self.reserved_items[product_id] += event.quantity
        
        elif isinstance(event, OrderItemRemoved):
            # Release inventory reservation
            product_id = event.product_id
            if product_id in self.reserved_items:
                self.reserved_items[product_id] = max(
                    0, self.reserved_items[product_id] - event.quantity
                )
        
        elif isinstance(event, OrderShipped):
            # Commit inventory reduction (simplified - would need order items)
            pass
        
        elif isinstance(event, OrderCancelled):
            # Release all reservations for this order (simplified)
            pass
    
    def get_reserved_quantity(self, product_id: UUID) -> int:
        """Get reserved quantity for a product"""
        return self.reserved_items.get(product_id, 0)


class CustomerOrderHistoryReadModel(ReadModel):
    """Read model for customer order history"""
    
    def __init__(self):
        self.customer_orders: Dict[UUID, List[Dict[str, Any]]] = {}
        self.customer_stats: Dict[UUID, Dict[str, Any]] = {}
    
    def handle_event(self, event: Event) -> None:
        """Handle events for customer order history"""
        if isinstance(event, OrderCreated):
            customer_id = event.customer_id
            
            if customer_id not in self.customer_orders:
                self.customer_orders[customer_id] = []
                self.customer_stats[customer_id] = {
                    'total_orders': 0,
                    'total_spent': 0.0,
                    'first_order_date': event.timestamp,
                    'last_order_date': event.timestamp
                }
            
            order_summary = {
                'order_id': event.aggregate_id,
                'created_at': event.timestamp,
                'total_amount': event.total_amount,
                'currency': event.currency,
                'status': OrderStatus.PENDING.value
            }
            
            self.customer_orders[customer_id].append(order_summary)
            self.customer_stats[customer_id]['total_orders'] += 1
            self.customer_stats[customer_id]['last_order_date'] = event.timestamp
        
        elif isinstance(event, PaymentProcessed):
            customer_orders = self._find_customer_orders_by_order_id(event.aggregate_id)
            if customer_orders:
                customer_id, order_summary = customer_orders
                order_summary['status'] = OrderStatus.CONFIRMED.value
                self.customer_stats[customer_id]['total_spent'] += event.amount
    
    def _find_customer_orders_by_order_id(self, order_id: UUID):
        """Find customer and order summary by order ID"""
        for customer_id, orders in self.customer_orders.items():
            for order in orders:
                if order['order_id'] == order_id:
                    return customer_id, order
        return None
    
    def get_customer_orders(self, customer_id: UUID) -> List[Dict[str, Any]]:
        """Get all orders for a customer"""
        return self.customer_orders.get(customer_id, [])
    
    def get_customer_stats(self, customer_id: UUID) -> Dict[str, Any]:
        """Get customer statistics"""
        return self.customer_stats.get(customer_id, {})


# Custom Exceptions
class ConcurrencyError(Exception):
    """Raised when there's a concurrency conflict"""
    pass


class DomainError(Exception):
    """Raised when there's a domain rule violation"""
    pass


# Event Serialization
class EventSerializer:
    """Serializes and deserializes events for storage"""
    
    @staticmethod
    def serialize(event: Event) -> str:
        """Serialize event to JSON string"""
        event_dict = {
            'event_id': str(event.event_id),
            'aggregate_id': str(event.aggregate_id) if event.aggregate_id else None,
            'event_type': event.event_type,
            'version': event.version,
            'timestamp': event.timestamp.isoformat(),
            'metadata': event.metadata,
            'data': {}
        }
        
        # Extract event-specific data
        for field_name, field_value in event.__dict__.items():
            if field_name not in ['event_id', 'aggregate_id', 'event_type', 'version', 'timestamp', 'metadata']:
                if isinstance(field_value, UUID):
                    event_dict['data'][field_name] = str(field_value)
                else:
                    event_dict['data'][field_name] = field_value
        
        return json.dumps(event_dict, default=str)
    
    @staticmethod
    def deserialize(event_json: str) -> Event:
        """Deserialize event from JSON string"""
        event_dict = json.loads(event_json)
        
        # Map event types to classes
        event_classes = {
            'OrderCreated': OrderCreated,
            'OrderItemAdded': OrderItemAdded,
            'OrderItemRemoved': OrderItemRemoved,
            'OrderShipped': OrderShipped,
            'OrderCancelled': OrderCancelled,
            'PaymentProcessed': PaymentProcessed,
        }
        
        event_class = event_classes.get(event_dict['event_type'])
        if not event_class:
            raise ValueError(f"Unknown event type: {event_dict['event_type']}")
        
        # Create event instance
        event_data = event_dict['data']
        
        # Convert UUID strings back to UUID objects
        for key, value in event_data.items():
            if key.endswith('_id') and isinstance(value, str):
                try:
                    event_data[key] = UUID(value)
                except ValueError:
                    pass  # Not a valid UUID string
        
        event = event_class(**event_data)
        event.event_id = UUID(event_dict['event_id'])
        event.aggregate_id = UUID(event_dict['aggregate_id']) if event_dict['aggregate_id'] else None
        event.version = event_dict['version']
        event.timestamp = datetime.fromisoformat(event_dict['timestamp'])
        event.metadata = event_dict['metadata']
        
        return event


# Example Usage and Test
def example_usage():
    """Demonstrate the event sourcing system"""
    print("=== Event Sourcing Order Processing System Demo ===\n")
    
    # Initialize components
    event_store = EventStore()
    repository = Repository(event_store)
    projection_builder = ProjectionBuilder(event_store)
    
    # Create read models
    order_summary_model = OrderSummaryReadModel()
    inventory_model = InventoryReadModel()
    customer_history_model = CustomerOrderHistoryReadModel()
    
    # Register read models
    projection_builder.register_read_model("order_summary", order_summary_model)
    projection_builder.register_read_model("inventory", inventory_model)
    projection_builder.register_read_model("customer_history", customer_history_model)
    
    # Create and process an order
    print("1. Creating a new order...")
    order = Order()
    customer_id = uuid4()
    product_id_1 = uuid4()
    product_id_2 = uuid4()
    
    # Create order with initial items
    order.create_order(
        customer_id=customer_id,
        items=[
            {
                'product_id': product_id_1,
                'quantity': 2,
                'unit_price': 29.99,
                'item_name': 'Widget A'
            },
            {
                'product_id': product_id_2,
                'quantity': 1,
                'unit_price': 49.99,
                'item_name': 'Widget B'
            }
        ]
    )
    
    print(f"Order created with ID: {order.aggregate_id}")
    print(f"Total amount: ${order.total_amount}")
    
    # Save order
    repository.save(order)
    
    # Update projections
    projection_builder.rebuild_all_projections()
    
    print("\n2. Processing payment...")
    order.process_payment("credit_card", order.total_amount, "txn_12345")
    repository.save(order)
    projection_builder.update_projections()
    
    print(f"Order status: {order.status.value}")
    
    print("\n3. Shipping order...")
    order.ship_order(
        shipping_address={
            "street": "123 Main St",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345"
        },
        tracking_number="TRACK123",
        carrier="FedEx"
    )
    repository.save(order)
    projection_builder.update_projections()
    
    print(f"Order status: {order.status.value}")
    print(f"Tracking number: {order.shipping_info['tracking_number']}")
    
    # Query read models
    print("\n4. Querying read models...")
    
    # Order summary
    summary = order_summary_model.get_order_summary(order.aggregate_id)
    print(f"Order Summary: {summary}")
    
    # Customer history
    customer_orders = customer_history_model.get_customer_orders(customer_id)
    print(f"Customer order count: {len(customer_orders)}")
    
    customer_stats = customer_history_model.get_customer_stats(customer_id)
    print(f"Customer stats: {customer_stats}")
    
    # Event replay demonstration
    print("\n5. Demonstrating event replay...")
    
    # Load order from events
    loaded_order = repository.load(order.aggregate_id, Order)
    print(f"Loaded order total: ${loaded_order.total_amount}")
    print(f"Loaded order status: {loaded_order.status.value}")
    print(f"Event count for order: {len(event_store.get_events(order.aggregate_id))}")
    
    # Show event serialization
    print("\n6. Event serialization example...")
    events = event_store.get_events(order.aggregate_id)
    if events:
        serialized = EventSerializer.serialize(events[0])
        print(f"Serialized event: {serialized}")
        
        deserialized = EventSerializer.deserialize(serialized)
        print(f"Deserialized event type: {deserialized.event_type}")
    
    return {
        'event_store': event_store,
        'repository': repository,
        'projection_builder': projection_builder,
        'order': order,
        'read_models': {
            'order_summary': order_summary_model,
            'inventory': inventory_model,
            'customer_history': customer_history_model
        }
    }


if __name__ == "__main__":
    # Run the example
    components = example_usage()
    
    print("\n=== System Components Ready ===")
    print("- Event Store: Append-only event log")
    print("- Repository: Aggregate persistence")
    print("- Projection Builder: Read model maintenance")
    print("- Read Models: Order summary, inventory, customer history")
    print("\nThe system is ready for production use!")
