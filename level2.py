# filename: order_processing.py

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Type, Protocol, get_args
import uuid

# ==============================================================================
# 1. Event Schema
#    - Events are immutable facts about something that happened in the system.
#    - We'll use a base class for events and a few specific event types.
# ==============================================================================

class Event(Protocol):
    """Protocol for all events to ensure they have an aggregate_id and version."""
    aggregate_id: str
    version: int
    timestamp: datetime = field(default_factory=datetime.utcnow)

@dataclass(frozen=True)
class OrderCreated:
    """Event: Represents the creation of a new order."""
    aggregate_id: str
    version: int
    customer_name: str
    timestamp: datetime = field(default_factory=datetime.utcnow)

@dataclass(frozen=True)
class ItemAddedToOrder:
    """Event: Represents an item being added to an existing order."""
    aggregate_id: str
    version: int
    item_name: str
    quantity: int
    timestamp: datetime = field(default_factory=datetime.utcnow)

@dataclass(frozen=True)
class OrderShipped:
    """Event: Represents an order being shipped."""
    aggregate_id: str
    version: int
    timestamp: datetime = field(default_factory=datetime.utcnow)

# ==============================================================================
# 2. Aggregate Root
#    - The 'Order' is our aggregate root. It's the object that enforces business rules.
#    - It doesn't store its state directly; it's reconstructed from events.
# ==============================================================================

class OrderAggregate:
    """
    The OrderAggregate is our "write model". It contains the logic for state
    transitions and validates commands. It's reconstructed from its event stream.
    """
    def __init__(self, order_id: str):
        # We start with a default, initial state.
        self.id = order_id
        self.version = 0
        self.customer_name = ""
        self.status = "PENDING"
        self.items = {}

    def _apply(self, event: Event):
        """
        Internal method to apply an event and update the aggregate's state.
        This is the core of event sourcing's state management.
        """
        # A dictionary-based dispatch pattern for cleaner event handling.
        handler_map = {
            OrderCreated: self._handle_order_created,
            ItemAddedToOrder: self._handle_item_added,
            OrderShipped: self._handle_order_shipped
        }
        handler = handler_map.get(type(event))
        if handler:
            handler(event)
        self.version = event.version

    def _handle_order_created(self, event: OrderCreated):
        self.customer_name = event.customer_name
        self.status = "CREATED"

    def _handle_item_added(self, event: ItemAddedToOrder):
        self.items[event.item_name] = self.items.get(event.item_name, 0) + event.quantity

    def _handle_order_shipped(self, event: OrderShipped):
        self.status = "SHIPPED"

    @classmethod
    def from_events(cls, events: List[Event]):
        """
        Reconstructs the aggregate's state by replaying its entire event history.
        """
        if not events:
            raise ValueError("Cannot build aggregate from an empty event stream.")
        
        # The first event must be a creation event to get the ID.
        first_event = events[0]
        if not isinstance(first_event, OrderCreated):
            raise TypeError("First event must be an OrderCreated event.")

        aggregate = cls(first_event.aggregate_id)
        for event in events:
            aggregate._apply(event)
        return aggregate

    def create_order(self, customer_name: str) -> Event:
        """Business logic: creates an order and returns the event."""
        if self.version != 0:
            raise ValueError("Order already exists.")
        new_version = self.version + 1
        return OrderCreated(
            aggregate_id=self.id,
            version=new_version,
            customer_name=customer_name
        )

    def add_item(self, item_name: str, quantity: int) -> Event:
        """Business logic: adds an item and returns the event."""
        if self.status == "SHIPPED":
            raise ValueError("Cannot add items to a shipped order.")
        new_version = self.version + 1
        return ItemAddedToOrder(
            aggregate_id=self.id,
            version=new_version,
            item_name=item_name,
            quantity=quantity
        )
    
    def ship_order(self) -> Event:
        """Business logic: ships an order and returns the event."""
        if self.status == "SHIPPED":
            raise ValueError("Order is already shipped.")
        if not self.items:
            raise ValueError("Cannot ship an order with no items.")
        new_version = self.version + 1
        return OrderShipped(
            aggregate_id=self.id,
            version=new_version
        )

# ==============================================================================
# 3. Event Store (Append-only log)
#    - Our single source of truth.
# ==============================================================================

class EventStore:
    """
    A simple in-memory implementation of an append-only event log.
    In a real system, this would be a database like Cassandra or MongoDB.
    """
    def __init__(self):
        # A dictionary mapping aggregate_id to a list of its events.
        self._events: Dict[str, List[Event]] = {}
    
    def append_events(self, aggregate_id: str, events: List[Event], expected_version: int):
        """
        Appends new events to the log, ensuring optimistic concurrency control.
        """
        if aggregate_id not in self._events:
            self._events[aggregate_id] = []
        
        current_version = len(self._events[aggregate_id])
        if current_version != expected_version:
            raise ValueError("Optimistic concurrency conflict! Version mismatch.")
        
        # Check that the events are in the correct version sequence.
        for event in events:
            if event.version != current_version + 1:
                raise ValueError("Event version does not follow the sequence.")
            current_version += 1
            self._events[aggregate_id].append(event)
        
    def get_events_for_aggregate(self, aggregate_id: str) -> List[Event]:
        """Retrieves the full event stream for a given aggregate."""
        return self._events.get(aggregate_id, [])

# ==============================================================================
# Main Application Flow
# ==============================================================================

def main():
    print("=== Starting Order Processing System ===")
    event_store = EventStore()
    order_id = str(uuid.uuid4())

    try:
        # Step 1: Create an order
        print(f"\nCreating a new order with ID: {order_id}")
        order_to_create = OrderAggregate(order_id)
        created_event = order_to_create.create_order(customer_name="Alice")
        event_store.append_events(
            aggregate_id=order_id,
            events=[created_event],
            expected_version=0
        )
        print("Order created event successfully appended.")

        # Step 2: Add items to the order
        print(f"\nAdding items to order ID: {order_id}")
        # Rebuild the aggregate from its events to get the current state.
        events = event_store.get_events_for_aggregate(order_id)
        order_to_update = OrderAggregate.from_events(events)
        
        item_event_1 = order_to_update.add_item("T-shirt", 2)
        event_store.append_events(
            aggregate_id=order_id,
            events=[item_event_1],
            expected_version=len(events)
        )
        
        events = event_store.get_events_for_aggregate(order_id)
        order_to_update = OrderAggregate.from_events(events)
        item_event_2 = order_to_update.add_item("Jeans", 1)
        event_store.append_events(
            aggregate_id=order_id,
            events=[item_event_2],
            expected_version=len(events)
        )
        print("Items added events successfully appended.")

        # Step 3: Ship the order
        print(f"\nShipping order ID: {order_id}")
        events = event_store.get_events_for_aggregate(order_id)
        order_to_ship = OrderAggregate.from_events(events)
        shipped_event = order_to_ship.ship_order()
        event_store.append_events(
            aggregate_id=order_id,
            events=[shipped_event],
            expected_version=len(events)
        )
        print("Order shipped event successfully appended.")

        # Step 4: Rebuild the final state to view it
        print(f"\nRebuilding final state for order ID: {order_id}")
        final_events = event_store.get_events_for_aggregate(order_id)
        final_order_state = OrderAggregate.from_events(final_events)
        
        print("\n=== Final Order State ===")
        print(f"ID: {final_order_state.id}")
        print(f"Version: {final_order_state.version}")
        print(f"Customer: {final_order_state.customer_name}")
        print(f"Status: {final_order_state.status}")
        print(f"Items: {final_order_state.items}")

    except ValueError as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
