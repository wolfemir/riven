import os
import threading
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from queue import Empty
from threading import Lock
from typing import Dict, List

from loguru import logger
from pydantic import BaseModel

from program.db import db_functions
from program.db.db import db
from program.managers.sse_manager import sse_manager
from program.types import Event


class EventUpdate(BaseModel):
    item_id: int
    emitted_by: str
    run_at: str


class EventManager:
    """
    Manages the execution of services and the handling of events.
    """
    def __init__(self):
        self._executors: Dict[str, ThreadPoolExecutor] = {}  # Using dict for O(1) lookup
        self._futures: set[Future] = set()  # Using set for O(1) operations
        self._queued_events: set[Event] = set()
        self._running_events: set[Event] = set()
        self._canceled_futures: set[Future] = set()
        self._content_queue: list[Event] = []
        self.mutex = Lock()
        self._event_cache = {}  # Cache for event updates
        self._cache_lock = Lock()

    def _find_or_create_executor(self, service_cls) -> ThreadPoolExecutor:
        """Find or create a ThreadPoolExecutor for the given service class."""
        service_name = service_cls.__name__
        
        with self.mutex:
            if service_name in self._executors:
                return self._executors[service_name]
            
            env_var_name = f"{service_name.upper()}_MAX_WORKERS"
            max_workers = int(os.environ.get(env_var_name, 3))  # Default to 3 workers
            
            executor = ThreadPoolExecutor(
                thread_name_prefix=service_name,
                max_workers=max_workers
            )
            self._executors[service_name] = executor
            logger.debug(f"Created executor for {service_name} with {max_workers} max workers.")
            return executor

    def _process_future(self, future, service):
        """Process the result of a future with improved error handling and caching."""
        if future.cancelled():
            with self.mutex:
                self._futures.discard(future)
            return

        try:
            result = future.result()
            
            with self.mutex:
                self._futures.discard(future)
                if hasattr(future, "event"):
                    self._running_events.discard(future.event)

            if isinstance(result, tuple):
                item_id, timestamp = result
            else:
                item_id, timestamp = result, datetime.now()

            if item_id and not future.cancellation_event.is_set():
                new_event = Event(
                    emitted_by=service,
                    item_id=item_id,
                    run_at=timestamp
                )
                self.add_event(new_event)
                
                # Update cache
                with self._cache_lock:
                    self._event_cache[item_id] = {
                        "item_id": item_id,
                        "emitted_by": service.__name__,
                        "run_at": timestamp.isoformat()
                    }
                
            # Batch update SSE events
            if len(self._event_cache) >= 10:  # Batch size
                self._publish_event_updates()

        except Exception as e:
            logger.error(f"Error in future for {service.__name__}: {str(e)}")
            if logger.level("DEBUG").no <= logger.level("DEBUG").no:
                logger.debug(traceback.format_exc())

    def _publish_event_updates(self):
        """Batch publish event updates to SSE."""
        with self._cache_lock:
            if self._event_cache:
                updates = list(self._event_cache.values())
                sse_manager.publish_event("event_update", updates)
                self._event_cache.clear()

    def add_event_to_queue(self, event: Event, log_message=True):
        """Add an event to the queue with improved efficiency."""
        with self.mutex:
            if event not in self._queued_events:
                self._queued_events.add(event)
                if log_message:
                    logger.debug(f"Added {event.log_message} to the queue.")

    def remove_event_from_queue(self, event: Event):
        with self.mutex:
            self._queued_events.discard(event)
            logger.debug(f"Removed {event.log_message} from the queue.")

    def remove_event_from_running(self, event: Event):
        with self.mutex:
            if event in self._running_events:
                self._running_events.discard(event)
                logger.debug(f"Removed {event.log_message} from running events.")

    def remove_id_from_queue(self, item_id: str):
        """
        Removes an item from the queue.

        Args:
            item (MediaItem): The event item to remove from the queue.
        """
        for event in list(self._queued_events):
            if event.item_id == item_id:
                self.remove_event_from_queue(event)

    def remove_id_from_running(self, item_id: str):
        """
        Removes an item from the running events.

        Args:
            item (MediaItem): The event item to remove from the running events.
        """
        for event in list(self._running_events):
            if event.item_id == item_id:
                self.remove_event_from_running(event)

    def remove_id_from_queues(self, item_id: str):
        """
        Removes an item from both the queue and the running events.

        Args:
            item_id: The event item to remove from both the queue and the running events.
        """
        self.remove_id_from_queue(item_id)
        self.remove_id_from_running(item_id)

    def submit_job(self, service, program, event=None):
        """
        Submits a job to be executed by the service.

        Args:
            service (type): The service class to execute.
            program (Program): The program containing the service.
            item (Event, optional): The event item to process. Defaults to None.
        """
        log_message = f"Submitting service {service.__name__} to be executed"
        item_id = None
        # Content services dont provide an event.
        if event:
            log_message += f" with {event.log_message}"
        logger.debug(log_message)

        cancellation_event = threading.Event()
        executor = self._find_or_create_executor(service)
        future = executor.submit(db_functions.run_thread_with_db_item, program.all_services[service].run, service, program, event, cancellation_event)
        future.cancellation_event = cancellation_event
        if event:
            future.event = event
        with self.mutex:
            self._futures.add(future)
        sse_manager.publish_event("event_update", self.get_event_updates())
        future.add_done_callback(lambda f:self._process_future(f, service))

    def cancel_job(self, item_id: str, suppress_logs=False):
        """
        Cancels a job associated with the given item.

        Args:
            item_id (int): The event item whose job needs to be canceled.
            suppress_logs (bool): If True, suppresses debug logging for this operation.
        """
        with db.Session() as session:
            item_id, related_ids = db_functions.get_item_ids(session, item_id)
            ids_to_cancel = set([item_id] + related_ids)

            for future in list(self._futures):
                future_item_id = None
                future_related_ids = []

                if hasattr(future, "event") and hasattr(future.event, "item_id"):
                    future_item = future.event.item_id
                    future_item_id, future_related_ids = db_functions.get_item_ids(session, future_item)

                if future_item_id in ids_to_cancel or any(rid in ids_to_cancel for rid in future_related_ids):
                    self.remove_id_from_queues(future_item)
                    if not future.done() and not future.cancelled():
                        try:
                            future.cancellation_event.set()
                            future.cancel()
                            with self.mutex:
                                self._canceled_futures.add(future)
                        except Exception as e:
                            if not suppress_logs:
                                logger.error(f"Error cancelling future for {future_item.log_string}: {str(e)}")


        logger.debug(f"Canceled jobs for Item ID {item_id} and its children.")

    def next(self):
        """
        Get the next event in the queue with an optional timeout.

        Raises:
            Empty: If the queue is empty.

        Returns:
            Event: The next event in the queue.
        """
        while True:
            if self._queued_events:
                with self.mutex:
                    events = list(self._queued_events)
                    events.sort(key=lambda event: event.run_at)
                    if datetime.now() >= events[0].run_at:
                        event = events.pop(0)
                        self._queued_events.discard(event)
                        return event
            raise Empty

    def _id_in_queue(self, _id):
        """
        Checks if an item with the given ID is in the queue.

        Args:
            _id (str): The ID of the item to check.

        Returns:
            bool: True if the item is in the queue, False otherwise.
        """
        return any(event.item_id == _id for event in self._queued_events)

    def _id_in_running_events(self, _id):
        """
        Checks if an item with the given ID is in the running events.

        Args:
            _id (str): The ID of the item to check.

        Returns:
            bool: True if the item is in the running events, False otherwise.
        """
        return any(event.item_id == _id for event in self._running_events)

    def add_event(self, event: Event):
        """
        Adds an event to the queue if it is not already present in the queue or running events.

        Args:
            event (Event): The event to add to the queue.

        Returns:
            bool: True if the event was added to the queue, False if it was already present.
        """
        # Check if the event's item is a show and its seasons or episodes are in the queue or running
        with db.Session() as session:
            item_id, related_ids = db_functions.get_item_ids(session, event.item_id)
        if item_id:
            if self._id_in_queue(item_id):
                logger.debug(f"Item ID {item_id} is already in the queue, skipping.")
                return False
            if self._id_in_running_events(item_id):
                logger.debug(f"Item ID {item_id} is already running, skipping.")
                return False
            for related_id in related_ids:
                if self._id_in_queue(related_id) or self._id_in_running_events(related_id):
                    logger.debug(f"Child of Item ID {item_id} is already in the queue or running, skipping.")
                    return False
        else:
            imdb_id = event.content_item.imdb_id
            if any(event.content_item and event.content_item.imdb_id == imdb_id for event in self._queued_events):
                logger.debug(f"Content Item with IMDB ID {imdb_id} is already in queue, skipping.")
                return False
            if any(
                event.content_item and event.content_item.imdb_id == imdb_id for event in self._running_events
            ):
                logger.debug(f"Content Item with IMDB ID {imdb_id} is already running, skipping.")
                return False

        self.add_event_to_queue(event)
        return True

    def add_event_to_running(self, event: Event):
        """Add an event to the running events set."""
        with self.mutex:
            if event not in self._running_events:
                self._running_events.add(event)
                logger.debug(f"Added {event.log_message} to running events.")

    def add_item(self, item, service="Manual"):
        """
        Adds an item to the queue as an event.

        Args:
            item (MediaItem): The item to add to the queue as an event.
        """
        # For now lets just support imdb_ids...
        if not db_functions.get_item_by_external_id(imdb_id=item.imdb_id):
            if self.add_event(Event(service, content_item=item)):
                logger.debug(f"Added item with IMDB ID {item.imdb_id} to the queue.")

    def get_event_updates(self) -> Dict[str, List[str]]:
        events = [future.event for future in self._futures if hasattr(future, "event")]
        event_types = ["Scraping", "Downloader", "Symlinker", "Updater", "PostProcessing"]

        updates = {event_type: [] for event_type in event_types}
        for event in events:
            table = updates.get(event.emitted_by.__name__, None)
            if table is not None:
                table.append(event.item_id)

        return updates