import time
import threading
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, Union, Any
from collections import defaultdict
from datetime import timedelta
import random

from loguru import logger
from pydantic import BaseModel
from requests import Session
from RTN import parse

from program.media.item import MediaItem
from program.media.stream import Stream
from program.settings.manager import settings_manager
from program.utils.request import (
    BaseRequestHandler,
    HttpMethod,
    ResponseType,
    create_service_session,
    get_rate_limit_params,
)

from .shared import (
    DownloadCachedStreamResult,
    DownloaderBase,
    FileFinder,
    premium_days_left,
    TorrentAddResult
)

# Video file extensions to look for
VIDEO_EXTENSIONS = {
    'mkv', 'mp4', 'avi', 'mov', 'wmv', 'flv', 'm4v', 'mpg', 'mpeg', 
    'webm', 'vob', 'ts', 'm2ts', 'mts'
}

class RDTorrentStatus(str, Enum):
    """Real-Debrid torrent status enumeration"""
    MAGNET_ERROR = "magnet_error"
    MAGNET_CONVERSION = "magnet_conversion"
    WAITING_FILES_SELECTION = "waiting_files_selection"
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    ERROR = "error"
    VIRUS = "virus"
    COMPRESSING = "compressing"
    UPLOADING = "uploading"
    DEAD = "dead"

class RDTorrent(BaseModel):
    """Real-Debrid torrent model"""
    id: str
    hash: str
    filename: str
    bytes: int
    status: RDTorrentStatus
    added: datetime
    links: List[str]
    ended: Optional[datetime] = None
    speed: Optional[int] = None
    seeders: Optional[int] = None

class RealDebridError(Exception):
    """Base exception for Real-Debrid errors"""
    pass

class TorrentNotFoundError(RealDebridError):
    """Raised when a torrent is not found on Real-Debrid"""
    pass

class InvalidFileIDError(RealDebridError):
    """Raised when an invalid file ID is provided"""
    pass

class RealDebridRateLimiter:
    """Thread-safe rate limiter for Real-Debrid API requests."""
    
    def __init__(self, requests_per_minute: int = 60):
        self.requests_per_minute = requests_per_minute
        self.request_count = 0
        self.last_reset = time.time()
        self._lock = threading.Lock()
        self._running = True
        self.backoff_time = 0
        self.consecutive_failures = 0
        self._start_reset_thread()
    
    def _start_reset_thread(self):
        """Start background thread to reset request counter."""
        def reset_counter():
            while self._running:
                time.sleep(60)  # Wait for 1 minute
                with self._lock:
                    current_time = time.time()
                    if current_time - self.last_reset >= 60:
                        logger.debug(f"🔄 Resetting rate limit counter (was {self.request_count})")
                        self.request_count = 0
                        self.last_reset = current_time
        
        self._reset_thread = threading.Thread(target=reset_counter, daemon=True)
        self._reset_thread.start()
    
    def acquire(self, wait: bool = True) -> bool:
        """
        Acquire permission to make an API request with exponential backoff.
        Args:
            wait: If True, wait until a request slot is available
        Returns:
            True if request is allowed, False if rate limit reached and wait=False
        """
        while True:
            with self._lock:
                current_time = time.time()
                
                # If we're in backoff period, wait
                if self.backoff_time > current_time:
                    if not wait:
                        logger.warning("❌ In backoff period and wait=False")
                        return False
                    backoff_remaining = self.backoff_time - current_time
                    logger.debug(f"⏳ In backoff period, waiting {backoff_remaining:.1f}s...")
                    time.sleep(min(backoff_remaining, 1))
                    continue
                
                # Reset counter if minute has passed
                if current_time - self.last_reset >= 60:
                    logger.debug(f"🔄 Resetting rate limit counter (was {self.request_count})")
                    self.request_count = 0
                    self.last_reset = current_time
                    # Also reduce consecutive failures if we've gone a full minute without issues
                    if self.consecutive_failures > 0:
                        self.consecutive_failures = max(0, self.consecutive_failures - 1)
                
                # Check if we can make a request
                if self.request_count < self.requests_per_minute:
                    self.request_count += 1
                    logger.debug(f"✅ Rate limit request approved ({self.request_count}/{self.requests_per_minute})")
                    return True
                
                if not wait:
                    logger.warning("❌ Rate limit reached and wait=False")
                    return False
            
            # Calculate wait time with jitter
            jitter = random.uniform(0.5, 1.5)
            wait_time = (60 - (current_time - self.last_reset)) * jitter
            wait_time = min(max(1, wait_time), 10)  # Bound between 1 and 10 seconds
            
            logger.debug(f"⏳ Rate limit reached, waiting {wait_time:.1f}s...")
            time.sleep(wait_time)

    def mark_failure(self):
        """Mark a rate limit failure and implement exponential backoff"""
        with self._lock:
            self.consecutive_failures += 1
            # Calculate backoff time: 2^failures seconds with max of 5 minutes
            backoff = min(2 ** self.consecutive_failures, 300)
            self.backoff_time = time.time() + backoff
            logger.warning(f"⚠️ Rate limit failure #{self.consecutive_failures}, backing off for {backoff}s")

    def mark_success(self):
        """Mark a successful request to reduce backoff"""
        with self._lock:
            if self.consecutive_failures > 0:
                self.consecutive_failures = max(0, self.consecutive_failures - 1)
                if self.consecutive_failures == 0:
                    self.backoff_time = 0
                    logger.debug("✅ Reset backoff after successful request")
    
    def shutdown(self):
        """Stop the reset thread."""
        self._running = False
        if self._reset_thread:
            self._reset_thread.join(timeout=1)

    def __enter__(self):
        """Enter the context manager by acquiring a rate limit slot"""
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and mark success/failure based on exception"""
        if exc_type is not None:
            self.mark_failure()
        else:
            self.mark_success()

class DownloadManager:
    """Manages concurrent downloads and threads"""
    def __init__(self, max_concurrent: int = 5):
        self.max_concurrent = max_concurrent
        self.active_downloads = 0
        self._lock = threading.Lock()
        self._download_complete = threading.Event()
        self.active_torrents = {}  # Track active torrents by ID
        
    def wait_for_slot(self, timeout: Optional[float] = None) -> bool:
        """Wait until a download slot is available.
        
        Args:
            timeout: Maximum time to wait in seconds, or None to wait indefinitely
            
        Returns:
            bool: True if slot acquired, False if timeout occurred
        """
        start_time = time.time()
        while True:
            with self._lock:
                if self.active_downloads < self.max_concurrent:
                    self.active_downloads += 1
                    logger.debug(f"✅ Download slot acquired ({self.active_downloads}/{self.max_concurrent})")
                    return True
            
            if timeout is not None:
                if time.time() - start_time > timeout:
                    logger.warning("❌ Timeout waiting for download slot")
                    return False
            
            logger.debug("⏳ Waiting for download slot...")
            time.sleep(1)
            
    def release_slot(self, torrent_id: Optional[str] = None):
        """Release a download slot and cleanup torrent tracking.
        
        Args:
            torrent_id: Optional torrent ID to remove from tracking
        """
        with self._lock:
            self.active_downloads = max(0, self.active_downloads - 1)
            if torrent_id and torrent_id in self.active_torrents:
                del self.active_torrents[torrent_id]
            
            if self.active_downloads == 0:
                self._download_complete.set()
                logger.debug("✨ All downloads complete")
            else:
                logger.debug(f"📊 Active downloads: {self.active_downloads}/{self.max_concurrent}")
    
    def add_torrent(self, torrent_id: str, info: dict):
        """Track a new active torrent.
        
        Args:
            torrent_id: The torrent ID
            info: Torrent information dictionary
        """
        with self._lock:
            self.active_torrents[torrent_id] = {
                'info': info,
                'start_time': time.time(),
                'last_progress': 0,
                'stalled_time': 0
            }
    
    def update_torrent(self, torrent_id: str, info: dict) -> bool:
        """Update torrent progress and check for stalled downloads.
        
        Args:
            torrent_id: The torrent ID
            info: Updated torrent information
            
        Returns:
            bool: True if download is progressing normally
        """
        with self._lock:
            if torrent_id not in self.active_torrents:
                return True
                
            data = self.active_torrents[torrent_id]
            current_progress = info.get('progress', 0)
            
            # Check if progress has improved
            if current_progress > data['last_progress']:
                data['stalled_time'] = 0
                data['last_progress'] = current_progress
                return True
            
            # Calculate stalled time
            elapsed = time.time() - data['start_time']
            data['stalled_time'] = elapsed
            
            # Return False if stalled for too long
            return data['stalled_time'] < 300  # 5 minutes
    
    def get_active_torrents(self) -> Dict[str, dict]:
        """Get copy of active torrents dictionary."""
        with self._lock:
            return self.active_torrents.copy()
    
    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """Wait for all downloads to complete.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            bool: True if all downloads completed, False if timeout occurred
        """
        return self._download_complete.wait(timeout=timeout)

class RealDebridAPI:
    """Real-Debrid API client with rate limiting and error handling."""
    
    def __init__(self, api_key: str, proxy_url: Optional[str] = None):
        self.api_key = api_key
        self.proxy_url = proxy_url
        self.rate_limiter = RealDebridRateLimiter()
        self.request_handler = self._create_request_handler()
    
    def _create_request_handler(self):
        """Create a request handler with proper headers and rate limiting."""
        session = create_service_session(self.proxy_url)
        session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/x-www-form-urlencoded'
        })
        return RealDebridRequestHandler(session, "https://api.real-debrid.com/rest/1.0")
    
    def _handle_error_response(self, response: dict) -> None:
        """Handle error responses from Real-Debrid API."""
        error = response.get('error')
        if error:
            error_code = error.get('code', 'unknown')
            error_message = error.get('message', 'Unknown error')
            
            if error_code in ('auth_required', 'bad_token'):
                raise RealDebridError(f"Authentication error: {error_message}")
            elif error_code == 'permission_denied':
                raise RealDebridError(f"Permission denied: {error_message}")
            elif error_code == 'not_found':
                raise RealDebridError(f"Resource not found: {error_message}")
            elif error_code == 'too_many_requests':
                raise RealDebridError(f"Rate limit exceeded: {error_message}")
            else:
                raise RealDebridError(f"API error ({error_code}): {error_message}")

class RealDebridRequestHandler(BaseRequestHandler):
    def __init__(self, session: Session, base_url: str, request_logging: bool = False):
        super().__init__(session, response_type=ResponseType.DICT, base_url=base_url, custom_exception=RealDebridError, request_logging=request_logging)

    def execute(self, method: HttpMethod, endpoint: str, **kwargs) -> Union[dict, list]:
        response = super()._request(method, endpoint, **kwargs)
        if response.status_code == 204:
            return {}
        if not response.data and not response.is_ok:
            raise RealDebridError("Invalid JSON response from RealDebrid")
        return response.data

class RealDebridDownloader(DownloaderBase):
    """Main Real-Debrid downloader class implementing DownloaderBase"""
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0
    DOWNLOAD_POLL_INTERVAL = 5  # seconds
    BASE_TIMEOUT = 300  # 5 minutes
    MAX_TIMEOUT = 1800  # 30 minutes
    TIMEOUT_PER_50MB = 10  # 10 seconds per 50MB
    MAX_QUEUE_ATTEMPTS = 6  # Maximum number of queued torrents before retrying item later
    CLEANUP_INTERVAL = 60  # Check every minute instead of 5 minutes
    CLEANUP_MINIMAL_PROGRESS_TIME = 900  # 15 minutes instead of 30
    CLEANUP_MINIMAL_PROGRESS_THRESHOLD = 5  # 5% instead of 1%
    CLEANUP_STUCK_UPLOAD_TIME = 1800  # 30 minutes instead of 1 hour
    CLEANUP_STUCK_COMPRESSION_TIME = 900  # 15 minutes instead of 30
    CLEANUP_BATCH_SIZE = 10  # Process deletions in batches
    CLEANUP_SPEED_THRESHOLD = 50000  # 50 KB/s minimum speed
    CLEANUP_INACTIVE_TIME = 300  # 5 minutes of inactivity
    MAX_CONCURRENT_TOTAL = 9  # Increased from 5 to 9
    MAX_CONCURRENT_PER_CONTENT = 4  # Increased from 2 to 4
    MAX_CONCURRENT_CONTENT = 5  # Maximum number of different content items downloading at once
    STATUS_CHECK_INTERVAL = 5  # seconds
    QUEUE_TIMEOUT = 30  # seconds
    MAX_ZERO_SEEDER_CHECKS = 2  # Maximum number of zero seeder checks
    MAX_PARALLEL_TORRENTS = 5  # Maximum number of parallel torrents to try
    INITIAL_CHECK_DELAY = 5  # Initial delay before checking each torrent status
    TORRENT_ADD_INTERVAL = 5  # Wait time between adding torrents
    PARALLEL_CHECK_INTERVAL = 15  # Check interval for all active torrents
    RATE_LIMIT_BACKOFF_BASE = 2.0  # Base for exponential backoff
    RATE_LIMIT_INITIAL_DELAY = 5.0  # Initial delay in seconds
    RATE_LIMIT_MAX_DELAY = 60.0  # Maximum delay in seconds
    RATE_LIMIT_MAX_RETRIES = 5  # Maximum number of retries for rate limited requests

    def __init__(self, api: RealDebridAPI):
        """Initialize Real-Debrid downloader with thread-safe components"""
        self.api = api
        self.key = "real_debrid"  # Add key attribute for API identification
        self.rate_limiter = RealDebridRateLimiter()
        self.download_manager = DownloadManager()
        self._lock = threading.Lock()
        self._current_attempts = []
        self.last_cleanup_time = 0
        self.cleanup_interval = 300  # 5 minutes between cleanups
        self.initialized = False
        self.download_complete = {}
        self.active_downloads = defaultdict(set)
        self.queue_attempts = {}
        self.scraping_settings = settings_manager.settings.scraping
        # Initialize FileFinder with our file attribute names
        self.file_finder = FileFinder("filename", "filesize")

    def initialize(self) -> bool:
        """Initialize the downloader"""
        try:
            if not self.api:
                logger.error("No API client provided")
                return False

            # Validate premium status
            if not self._validate_premium():
                logger.error("Account validation failed")
                return False

            self.initialized = True
            return True

        except Exception as e:
            logger.error(f"Failed to initialize Real-Debrid: {e}")
            return False

    def _cleanup(self) -> int:
        """Clean up torrents that are no longer needed"""
        try:
            current_time = datetime.now()
            if (current_time - self.last_cleanup_time).total_seconds() < self.CLEANUP_INTERVAL:
                return 0

            # Get current torrents
            torrents = self.api.request_handler.execute(HttpMethod.GET, "torrents")
            if not torrents:
                return 0

            # Get current downloads
            downloads = self.api.request_handler.execute(HttpMethod.GET, "downloads")

            # Get active torrents by status
            active_by_status = defaultdict(list)
            for torrent in torrents:
                status = torrent.get("status", "")
                active_by_status[status].append(torrent)

            # Get active torrent count by status
            active_count = defaultdict(int)
            for status, torrents in active_by_status.items():
                active_count[status] = len(torrents)

            # Get total active torrents
            total_active = sum(active_count.values())

            # Get limit from settings
            limit = self.MAX_CONCURRENT_TOTAL

            # Mark torrents for deletion
            to_delete = []
            for status, torrents in active_by_status.items():
                for torrent in torrents:
                    torrent_id = torrent.get("id", "")
                    filename = torrent.get("filename", "")
                    status = torrent.get("status", "")
                    progress = torrent.get("progress", 0)
                    speed = torrent.get("speed", 0)
                    seeders = torrent.get("seeders", 0)
                    time_elapsed = torrent.get("time_elapsed", 0)

                    # Case 1: Completed torrents
                    if status == RDTorrentStatus.DOWNLOADED.value:
                        reason = "download completed"
                        to_delete.append((0, torrent_id, reason, time_elapsed))

                    # Case 2: Stuck torrents
                    elif status == RDTorrentStatus.DOWNLOADING.value and speed == 0 and time_elapsed > self.CLEANUP_INACTIVE_TIME:
                        reason = "download is stuck (zero speed)"
                        to_delete.append((1, torrent_id, reason, time_elapsed))

                    # Case 3: Torrents with zero progress
                    elif status == RDTorrentStatus.DOWNLOADING.value and progress == 0 and time_elapsed > self.CLEANUP_MINIMAL_PROGRESS_TIME:
                        reason = "download has zero progress"
                        to_delete.append((2, torrent_id, reason, time_elapsed))

                    # Case 4: Torrents with minimal progress
                    elif status == RDTorrentStatus.DOWNLOADING.value and progress < self.CLEANUP_MINIMAL_PROGRESS_THRESHOLD and time_elapsed > self.CLEANUP_MINIMAL_PROGRESS_TIME:
                        reason = f"download has minimal progress ({progress}%)"
                        to_delete.append((3, torrent_id, reason, time_elapsed))

                    # Case 5: Stuck uploading torrents
                    elif status == RDTorrentStatus.UPLOADING.value and speed == 0 and time_elapsed > self.CLEANUP_STUCK_UPLOAD_TIME:
                        reason = "upload is stuck (zero speed)"
                        to_delete.append((4, torrent_id, reason, time_elapsed))

                    # Case 6: Stuck compressing torrents
                    elif status == RDTorrentStatus.COMPRESSING.value and speed == 0 and time_elapsed > self.CLEANUP_STUCK_COMPRESSION_TIME:
                        reason = "compression is stuck (zero speed)"
                        to_delete.append((5, torrent_id, reason, time_elapsed))

                    # Case 7: Torrents with no seeders
                    elif status == RDTorrentStatus.DOWNLOADING.value and seeders == 0 and time_elapsed > self.CLEANUP_INACTIVE_TIME:
                        reason = "download has no seeders"
                        to_delete.append((6, torrent_id, reason, time_elapsed))

                    # Case 8: Waiting files selection
                    elif status == RDTorrentStatus.WAITING_FILES_SELECTION.value:
                        reason = "waiting files selection"
                        to_delete.append((7, torrent_id, reason, time_elapsed))

            # If no torrents were marked for deletion but we're still over limit,
            # force delete the slowest/least progressed torrents
            if not to_delete and total_active > active_count["limit"]:
                logger.info("No torrents met deletion criteria but still over limit, using fallback cleanup")
                
                # First try to clean up just duplicates
                duplicates_only = True
                cleanup_attempts = 2  # Try duplicates first, then all torrents if needed
                
                while cleanup_attempts > 0:
                    # Collect all active torrents into a single list for sorting
                    all_active = []
                    seen_filenames = set()
                    
                    for status, torrents in active_by_status.items():
                        for t in torrents:
                            filename = t["filename"]
                            
                            # Skip non-duplicates on first pass
                            is_duplicate = filename in seen_filenames
                            if duplicates_only and not is_duplicate:
                                continue
                            
                            seen_filenames.add(filename)
                            
                            score = 0
                            # Prioritize keeping torrents with more progress
                            score += t["progress"] * 100
                            # And those with higher speeds
                            score += min(t["speed"] / 1024, 1000)  # Cap speed bonus at 1000
                            # And those with more seeders
                            score += t["seeders"] * 10
                            # Penalize older torrents slightly
                            score -= min(t["time_elapsed"] / 60, 60)  # Cap age penalty at 60 minutes
                            # Heavy penalty for duplicates
                            if is_duplicate:
                                score -= 5000  # Ensure duplicates are cleaned up first
                            
                            all_active.append({
                                "id": t["id"],
                                "score": score,
                                "stats": t,
                                "status": status,
                                "is_duplicate": is_duplicate
                            })
                    
                    if all_active:
                        # Sort by score (lowest first - these will be deleted)
                        all_active.sort(key=lambda x: x["score"])
                        
                        # Take enough torrents to get under the limit
                        to_remove = min(
                            len(all_active),  # Don't try to remove more than we have
                            total_active - active_count["limit"] + 1  # +1 for safety margin
                        )
                        
                        for torrent in all_active[:to_remove]:
                            stats = torrent["stats"]
                            reason = (f"fallback cleanup{' (duplicate)' if duplicates_only else ''} - {torrent['status']} "
                                    f"(progress: {stats['progress']}%, "
                                    f"speed: {stats['speed']/1024:.1f} KB/s, "
                                    f"seeders: {stats['seeders']}, "
                                    f"age: {stats['time_elapsed']/60:.1f}m)")
                            to_delete.append((0, torrent["id"], reason, stats["time_elapsed"]))
                            logger.info(f"Fallback cleanup marking: {stats['filename']} - {reason}")
                        
                        # If we found enough torrents to delete, we're done
                        if len(to_delete) >= (total_active - active_count["limit"]):
                            break
                    
                    # If we get here and duplicates_only is True, try again with all torrents
                    duplicates_only = False
                    cleanup_attempts -= 1
                
                # Log what we're about to delete
                if to_delete:
                    logger.info(f"Found {len(to_delete)} torrents to clean up, processing in batches of {self.CLEANUP_BATCH_SIZE}")
                    for _, _, reason, _ in to_delete[:5]:  # Log first 5 for debugging
                        logger.debug(f"Will delete: {reason}")
            
            # Convert to final format
            to_delete = [(t[1], t[2]) for t in to_delete]
            
            # Process deletion in batches
            while to_delete:
                batch = to_delete[:self.CLEANUP_BATCH_SIZE]
                to_delete = to_delete[self.CLEANUP_BATCH_SIZE:]
                cleaned += self._batch_delete_torrents(batch)
            
            # Update last cleanup time if any torrents were cleaned
            if cleaned > 0:
                self.last_cleanup_time = current_time
                logger.info(f"Cleaned up {cleaned} torrents")
            else:
                logger.warning("No torrents were cleaned up despite being over the limit!")
            
            return cleaned
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return 0

    def _batch_delete_torrents(self, torrents: List[Tuple[str, str]]) -> int:
        """Delete a batch of torrents efficiently.
        Args:
            torrents: List of (torrent_id, reason) tuples
        Returns:
            Number of successfully deleted torrents
        """
        deleted = 0
        for torrent_id, reason in torrents:
            try:
                # First try to delete associated downloads
                try:
                    downloads = self.api.request_handler.execute(HttpMethod.GET, "downloads")
                    for download in downloads:
                        if download.get("torrent_id") == torrent_id:
                            try:
                                self.api.request_handler.execute(HttpMethod.DELETE, f"downloads/delete/{download['id']}")
                                logger.debug(f"Deleted download {download['id']} associated with torrent {torrent_id}")
                            except Exception as e:
                                logger.warning(f"Failed to delete download {download['id']}: {e}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup downloads for torrent {torrent_id}: {e}")

                # Then delete the torrent
                self.api.request_handler.execute(HttpMethod.DELETE, f"torrents/delete/{torrent_id}")
                logger.info(f"Deleted torrent {torrent_id}: {reason}")
                deleted += 1
            except Exception as e:
                if "404" in str(e):
                    # Torrent was already deleted, count it as success
                    logger.debug(f"Torrent {torrent_id} was already deleted")
                    deleted += 1
                elif "401" in str(e):
                    logger.error("API token expired or invalid")
                    break  # Stop processing batch
                elif "403" in str(e):
                    logger.error("Account locked or permission denied")
                    break  # Stop processing batch
                else:
                    logger.error(f"Failed to delete torrent {torrent_id}: {e}")
        return deleted

    def _cleanup_downloads(self) -> int:
        """Clean up old downloads that are no longer needed.
        Returns number of downloads cleaned up."""
        
        # Check if enough time has passed since last cleanup
        current_time = datetime.now()
        if (current_time - self.last_cleanup_time).total_seconds() < self.CLEANUP_INTERVAL:
            return 0
            
        try:
            downloads = self.api.request_handler.execute(HttpMethod.GET, "downloads")
            if not isinstance(downloads, list):
                logger.error(f"Unexpected downloads response type: {type(downloads)}")
                return 0
                
            deleted = 0
            
            # Get current torrents for reference
            try:
                torrents = {t["id"]: t for t in self.api.request_handler.execute(HttpMethod.GET, "torrents")}
            except Exception as e:
                logger.warning(f"Failed to get torrents list for reference: {e}")
                torrents = {}
            
            # Track active downloads to update our counters
            active_by_content = {}
            
            for download in downloads:
                try:
                    if not isinstance(download, dict):
                        logger.warning(f"Unexpected download entry type: {type(download)}")
                        continue
                        
                    download_id = download.get("id")
                    torrent_id = download.get("torrent_id")
                    filename = download.get("filename", "unknown")
                    status = download.get("status", "unknown")
                    progress = download.get("progress", 0)
                    speed = download.get("speed", 0)
                    content_id = download.get("content_id")
                    
                    # Track active downloads
                    if status in ("downloading", "queued"):
                        if content_id:
                            active_by_content.setdefault(content_id, set()).add(download_id)
                    
                    # Never delete successfully downloaded files
                    if status == "downloaded":
                        if content_id:
                            self.download_complete[content_id] = True
                        continue
                    
                    reason = None
                    
                    # Case 1: No associated torrent ID (but not if downloaded or unknown status)
                    if not torrent_id and status not in ("downloaded", "unknown"):
                        reason = "orphaned download (no torrent ID)"
                    
                    # Case 2: Associated torrent no longer exists (but not if downloaded or unknown status)
                    elif torrent_id and torrent_id not in torrents and status not in ("downloaded", "unknown"):
                        reason = f"orphaned download (torrent {torrent_id} no longer exists)"
                    
                    # Case 3: Download failed or errored
                    elif status in ("error", "magnet_error", "virus", "dead", "waiting_files_selection"):
                        reason = f"download in {status} state"
                    
                    # Case 4: Zero progress downloads (excluding queued, downloaded, and unknown)
                    elif progress == 0 and status not in ("queued", "downloaded", "unknown") and speed == 0:
                        reason = "download has zero progress and speed"
                    
                    # Case 5: Stuck downloads (but not if already downloaded or unknown)
                    elif status == "downloading" and speed == 0 and progress < 100 and status not in ("downloaded", "unknown"):
                        reason = "download is stuck (zero speed)"
                    
                    if reason:
                        # Double check status hasn't changed to downloaded or unknown
                        try:
                            current = self.api.request_handler.execute(HttpMethod.GET, f"downloads/info/{download_id}")
                            if isinstance(current, dict):
                                current_status = current.get("status")
                                if current_status == "downloaded":
                                    logger.debug(f"⏭️ Skipping deletion of {self._color_text(download_id, 'yellow')} ({filename}): {self._color_text('status changed to downloaded', 'green')}")
                                    if content_id:
                                        self.download_complete[content_id] = True
                                    continue
                                elif current_status == "unknown":
                                    logger.debug(f"⏭️ Skipping deletion of {self._color_text(download_id, 'yellow')} ({filename}): {self._color_text('status is unknown', 'yellow')}")
                                    continue
                        except Exception as e:
                            logger.debug(f"❌ Failed to double-check download status for {self._color_text(download_id, 'red')}: {e}")
                        
                        try:
                            self.api.request_handler.execute(HttpMethod.DELETE, f"downloads/delete/{download_id}")
                            deleted += 1
                            logger.info(f"🗑️ Deleted download {self._color_text(download_id, 'yellow')} ({filename}): {reason}, status: {status}")
                            
                            # Update our tracking
                            if content_id:
                                if download_id in self.active_downloads[content_id]:
                                    self.active_downloads[content_id].remove(download_id)
                        except Exception as e:
                            if "404" in str(e):
                                deleted += 1  # Already deleted
                                logger.debug(f"🗑️ Download {self._color_text(download_id, 'yellow')} was already deleted")
                                # Update our tracking
                                if content_id and download_id in self.active_downloads[content_id]:
                                    self.active_downloads[content_id].remove(download_id)
                            elif "401" in str(e):
                                logger.error("API token expired or invalid")
                                break  # Stop processing
                            elif "403" in str(e):
                                logger.error("Account locked or permission denied")
                                break  # Stop processing
                            else:
                                logger.warning(f"Failed to delete download {download_id}: {e}")
                
                except Exception as e:
                    logger.warning(f"Failed to process download {download.get('id')}: {e}")
            
            # Update our active downloads tracking
            for content_id in list(self.active_downloads.keys()):
                actual_active = active_by_content.get(content_id, set())
                self.active_downloads[content_id] = actual_active
            
            if deleted:
                logger.info(f"Cleaned up {deleted} downloads")
                # Log current download counts
                total = sum(len(downloads) for downloads in self.active_downloads.values())
                logger.debug(f"Current download counts - Total: {total}, By content: {dict((k, len(v)) for k, v in self.active_downloads.items())}")
            return deleted
            
        except Exception as e:
            logger.error(f"Failed to cleanup downloads: {e}")
            return 0

    def _process_files(self, files: List[dict], item: Optional[MediaItem] = None) -> Dict[str, dict]:
        """Process and filter valid video files"""
        logger.debug(f"🔍 Processing {len(files)} files from Real-Debrid")
        result = {}
        
        # If no files yet, return empty result to trigger retry
        if not files:
            logger.debug("⚠️ No files available yet, will retry")
            return {}
        
        # Process all video files
        valid_videos = []
        
        # Log what extensions we're looking for
        logger.debug(f"Looking for files with extensions: {VIDEO_EXTENSIONS}")
        
        for file in files:
            path = file.get("path", "")
            name = path.split("/")[-1] if path else ""
            size = file.get("bytes", 0)
            file_id = str(file.get("id", ""))
            
            # Skip if no valid ID
            if not file_id:
                logger.debug(f"❌ Skipped file with no ID: {path}")
                continue
        
            # Skip sample files and unwanted files
            if "/sample/" in name.lower() or "sample" in name.lower():
                logger.debug(f"⏭️ Skipped sample file: {name}")
                continue
            
            # Extract extension and check if it's a video file
            extension = name.split(".")[-1].lower() if "." in name else ""
            is_video = extension in VIDEO_EXTENSIONS
            
            if is_video:
                # For shows, check if this file matches the requested episode
                if item and hasattr(item, 'type') and item.type == "episode":
                    # Parse episode info from filename
                    try:
                        parsed = parse(name)
                        if parsed.seasons and parsed.episodes:
                            # Check if season/episode numbers match
                            if item.parent.number in parsed.seasons and item.number in parsed.episodes:
                                valid_videos.append(file)
                                logger.debug(f"✅ Found matching episode: {name} (S{parsed.seasons[0]:02d}E{item.number:02d})")
                            else:
                                logger.debug(f"❌ Episode numbers don't match: {name} vs S{item.parent.number:02d}E{item.number:02d}")
                        else:
                            # If we can't parse episode info but it's a video file, include it
                            valid_videos.append(file)
                            logger.debug(f"⚠️ Could not parse episode info but including video file: {name}")
                    except Exception as e:
                        # If parsing fails but it's a video file, include it
                        valid_videos.append(file)
                        logger.debug(f"⚠️ Error parsing episode info but including video file {name}: {e}")
                else:
                    # For movies or when no item is provided, include all video files
                    valid_videos.append(file)
                    logger.debug(f"✅ Found valid video file: {name} (size: {self._format_size(size)}, id: {file_id})")
            else:
                # Log why file was rejected
                logger.debug(f"❌ Skipped non-video file: {name} (extension: {extension})")

        # Sort videos by size (largest first) to ensure main episodes/movies are prioritized
        valid_videos.sort(key=lambda x: x.get("bytes", 0), reverse=True)
        
        # Add all valid video files
        for video in valid_videos:
            path = video.get("path", "")
            file_id = str(video.get("id", ""))
            size = video.get("bytes", 0)
            
            # Extract parent folder name from path
            path_parts = path.split("/")
            parent_path = path_parts[-2] if len(path_parts) > 1 else ""
            
            result[file_id] = {
                "filename": path,
                "filesize": size,
                "parent_path": parent_path
            }
            logger.debug(f"📥 Selected file for download: {path} (size: {self._format_size(size)}, id: {file_id})")

        if not result:
            # Log all files for debugging
            logger.debug("❌ No valid video files found. Available files:")
            for file in files:
                path = file.get("path", "")
                name = path.split("/")[-1] if path else ""
                size = file.get("bytes", 0)
                extension = name.split(".")[-1].lower() if "." in name else ""
                logger.debug(f"  • {path} (size: {self._format_size(size)}, extension: {extension})")
        else:
            logger.debug(f"✨ Selected {len(result)} video files for download")

        return result

    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} PB"

    def select_files(self, torrent_id: str, file_ids: List[str]) -> bool:
        """Select files for download"""
        try:
            if not file_ids:
                logger.error("No files to select")
                return False

            # Log what we're about to select
            logger.debug(f"📑 Selecting files for torrent {self._color_text(torrent_id, 'cyan')}:")
            logger.debug(f"  • File IDs: {file_ids}")
            
            # Get current torrent info for comparison
            before_info = self._get_torrent_info(torrent_id)
            if before_info:
                logger.debug(f"  • Current status: {self._format_status(before_info.get('status', 'unknown'))}")

            data = {"files": ",".join(file_ids)}
            self.api.request_handler.execute(HttpMethod.POST, f"torrents/selectFiles/{torrent_id}", data=data)
            
            # Get updated torrent info
            after_info = self._get_torrent_info(torrent_id)
            if after_info:
                logger.debug(f"  • New status: {self._format_status(after_info.get('status', 'unknown'))}")
            
            logger.debug(f"✅ Successfully selected {len(file_ids)} files")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to select files: {e}")
            return False

    def download_cached_stream(self, item: MediaItem, stream: Stream) -> DownloadCachedStreamResult:
        """Download a stream from Real-Debrid with sequential parallel checking"""
        content_id = item.id if item else None

        # Check if we can start a new download
        if not self._can_start_download(content_id):
            return DownloadCachedStreamResult(
                success=False,
                error=f"Cannot start new download - content limit reached"
            )

        active_torrents = {}  # Track active torrents and their info
        alternative_streams = self._get_alternative_streams(item, stream, max_count=self.MAX_PARALLEL_TORRENTS-1)
        
        def add_and_check_torrent(stream_to_add):
            """Add a torrent and check its initial status"""
            result = self.add_torrent(stream_to_add)
            if not result or not result.torrent_id:
                return None
                
            torrent_id = result.torrent_id
            active_torrents[torrent_id] = {
                'stream': stream_to_add,
                'content_name': item.name if item else "Unknown",
                'added_time': time.time(),
            }
            
            logger.debug(f"🚀 Added torrent {self._color_text(torrent_id, 'cyan')}")
            
            # Wait initial delay
            time.sleep(self.INITIAL_CHECK_DELAY)
            
            # Check torrent status
            info = self._get_torrent_info(torrent_id)
            if info and info.get('status') == RDTorrentStatus.DOWNLOADED.value:
                logger.debug(f"✨ Torrent {self._color_text(torrent_id, 'green')} is ready for download")
                return torrent_id
            
            return None
        
        # Step 1: Try initial torrent
        logger.debug(f"📥 Trying initial torrent...")
        downloaded_id = add_and_check_torrent(stream)
        if downloaded_id:
            return self.wait_for_download(downloaded_id, content_id, item, stream)
        
        # Step 2: Try alternative torrents sequentially
        for idx, alt_stream in enumerate(alternative_streams, 1):
            if len(active_torrents) >= self.MAX_PARALLEL_TORRENTS:
                break
                
            logger.debug(f"📥 Trying alternative torrent {idx}/{len(alternative_streams)}...")
            downloaded_id = add_and_check_torrent(alt_stream)
            if downloaded_id:
                # Clean up other torrents
                for other_id in list(active_torrents.keys()):
                    if other_id != downloaded_id:
                        self.delete_torrent(other_id)
                return self.wait_for_download(downloaded_id, content_id, item, alt_stream)
            
            # Wait before adding next torrent
            if idx < len(alternative_streams):
                time.sleep(self.TORRENT_ADD_INTERVAL)
        
        # Step 3: Monitor all torrents
        start_time = time.time()
        last_check_time = 0
        best_torrent = None
        best_seeders = -1
        
        while active_torrents:
            current_time = time.time()
            
            # Check all torrents every PARALLEL_CHECK_INTERVAL seconds
            if current_time - last_check_time < self.PARALLEL_CHECK_INTERVAL:
                time.sleep(1)
                continue
                
            last_check_time = current_time
            logger.debug(f"🔍 Checking {len(active_torrents)} active torrents...")
            
            status_summary = []
            for torrent_id, data in list(active_torrents.items()):
                info = self._get_torrent_info(torrent_id)
                if not info:
                    continue
                    
                status = info.get('status', 'unknown')
                seeders = info.get('seeders', 0)
                speed = info.get('speed', 0)
                progress = info.get('progress', 0)
                content_name = data.get('content_name', 'Unknown')
                
                # Format speed nicely
                speed_str = f"{speed/1024/1024:.1f}MB/s" if speed > 1024*1024 else f"{speed/1024:.1f}KB/s" if speed > 0 else "0KB/s"
                
                # Add status details
                status_details = f"[{self._color_text(content_name, 'yellow')}] "
                status_details += f"{status} ({progress:.1f}%) "
                status_details += f"🌱{seeders} "  # Seeders with plant emoji
                status_details += f"⚡{speed_str}"  # Speed with lightning emoji
                status_summary.append(status_details)
            
            if status_summary:
                logger.debug("Torrent Status:\n" + "\n".join(f"  • {s}" for s in status_summary))
            
            for torrent_id, data in list(active_torrents.items()):
                info = self._get_torrent_info(torrent_id)
                if not info:
                    continue
                    
                status = info.get('status', 'unknown')
                seeders = info.get('seeders', 0)
                
                # Found a downloaded torrent
                if status == RDTorrentStatus.DOWNLOADED.value:
                    logger.debug(f"✅ Found downloaded torrent {self._color_text(torrent_id, 'green')}")
                    
                    # Clean up other torrents
                    for other_id in list(active_torrents.keys()):
                        if other_id != torrent_id:
                            self.delete_torrent(other_id)
                            
                    return self.wait_for_download(torrent_id, content_id, item, data['stream'])
                
                # Track best seeder count
                if seeders > best_seeders:
                    best_seeders = seeders
                    best_torrent = (torrent_id, data)
                    logger.debug(f"⭐ New best torrent {self._color_text(torrent_id, 'yellow')} with {seeders} seeders")
                    
                    # Clean up torrents with fewer seeders
                    for other_id, other_data in list(active_torrents.items()):
                        if other_id != torrent_id:
                            other_info = self._get_torrent_info(other_id)
                            if other_info and other_info.get('seeders', 0) < seeders:
                                logger.debug(f"🗑️ Removing torrent {self._color_text(other_id, 'red')} with fewer seeders ({other_info.get('seeders', 0)} < {seeders})")
                                self.delete_torrent(other_id)
                                del active_torrents[other_id]
                
                # Remove dead torrents
                if status in [RDTorrentStatus.MAGNET_ERROR.value, RDTorrentStatus.ERROR.value, RDTorrentStatus.DEAD.value]:
                    logger.debug(f"❌ Removing dead torrent {self._color_text(torrent_id, 'red')}")
                    del active_torrents[torrent_id]
                    self.delete_torrent(torrent_id)
            
            # If we have a best torrent, keep only that one
            if best_torrent and len(active_torrents) > 1:
                best_id, _ = best_torrent
                for other_id in list(active_torrents.keys()):
                    if other_id != best_id:
                        logger.debug(f"🗑️ Removing non-best torrent {self._color_text(other_id, 'red')}")
                        self.delete_torrent(other_id)
                        del active_torrents[other_id]
        
        # If we get here, no torrent succeeded
        return DownloadCachedStreamResult(success=False, error="No torrents downloaded successfully")

    def _get_retry_hours(self, scrape_times: int) -> float:
        """Get retry hours based on number of scrape attempts."""
        if scrape_times >= 10:
            return self.scraping_settings.after_10
        elif scrape_times >= 5:
            return self.scraping_settings.after_5
        elif scrape_times >= 2:
            return self.scraping_settings.after_2
        return 2.0  # Default to 2 hours

    def wait_for_download(self, torrent_id: str, content_id: str, item: MediaItem, stream: Stream) -> DownloadCachedStreamResult:
        """Wait for torrent to finish downloading"""
        start_time = time.time()
        last_check_time = time.time()
        zero_seeder_count = 0  # Track consecutive zero seeder checks
        queue_start_time = None  # Track when queue started
        
        while True:
            current_time = time.time()
            elapsed = current_time - start_time

            # Only check status every few seconds
            if current_time - last_check_time < self.STATUS_CHECK_INTERVAL:
                time.sleep(0.1)
                continue

            try:
                info = self.get_torrent_info(torrent_id)
                if not info:
                    return DownloadCachedStreamResult(success=False, error="Failed to get torrent info")

                status = info.get("status", "").lower()  # Convert to lowercase
                filename = info.get("filename", "unknown")
                progress = info.get("progress", 0)
                speed = info.get("speed", 0) / 1024 / 1024  # Convert to MB/s
                seeders = info.get("seeders", 0)

                logger.debug(f"📊 {self._format_progress(progress, speed, seeders)}\n"
                             f"🎬 Processing file: {filename}")

                # For any status, first check if we have valid files
                files = info.get("files", [])
                processed = self._process_files(files, item) if files else {}

                if status == RDTorrentStatus.WAITING_FILES_SELECTION.value:
                    if not files:
                        return DownloadCachedStreamResult(success=False, error="No files available in torrent")

                    if not processed:
                        return DownloadCachedStreamResult(success=False, error="No valid video files found in torrent")

                    # Select the files for download
                    file_ids = list(processed.keys())
                    try:
                        self.select_files(torrent_id, file_ids)
                        logger.debug(f"✅ Successfully selected {len(file_ids)} files")
                    except Exception as e:
                        return DownloadCachedStreamResult(success=False, error=f"Failed to select files: {e}")
                    
                    last_check_time = current_time
                    continue

                elif status == RDTorrentStatus.QUEUED.value:
                    if queue_start_time is None:
                        queue_start_time = current_time
                        logger.debug(f"⏳ {filename} queued for download")
                    
                    queue_time = current_time - queue_start_time
                    if queue_time > self.QUEUE_TIMEOUT:
                        return DownloadCachedStreamResult(success=False, error=f"Queue timeout after {queue_time:.1f}s")
                    
                    last_check_time = current_time
                    continue

                elif status == RDTorrentStatus.DOWNLOADING.value:
                    if progress is not None:
                        logger.debug(f"⬇️ {filename}")
                        logger.debug(f"📊 {self._format_progress(progress, speed, seeders)}")
                        
                        # Calculate timeout based on progress and file size
                        timeout = self._calculate_timeout(info, item)
                        if elapsed > timeout:
                            return DownloadCachedStreamResult(success=False, error=f"Download timeout after {elapsed:.1f}s")
                        
                        # Check for stalled download
                        if speed == 0:
                            if seeders == 0:
                                zero_seeder_count += 1
                                if zero_seeder_count >= self.MAX_ZERO_SEEDER_CHECKS:
                                    return DownloadCachedStreamResult(success=False, error="No seeders available")
                            else:
                                zero_seeder_count = 0  # Reset counter if we find seeders
                    
                    last_check_time = current_time
                    continue

                elif status == RDTorrentStatus.DOWNLOADED.value:
                    # Even if downloaded, ensure we have valid video files
                    if not processed:
                        return DownloadCachedStreamResult(success=False, error="No valid video files found in downloaded torrent")

                    # Add processed files to both info and container for compatibility
                    info["processed_files"] = processed
                    logger.debug(f"✅ Download complete: {filename}")

                    return DownloadCachedStreamResult(
                        success=True,
                        torrent_id=torrent_id,
                        info=info,
                        info_hash=stream.infohash,
                        container=processed  # Store processed files in container for compatibility
                    )

                elif status in (RDTorrentStatus.ERROR.value, RDTorrentStatus.MAGNET_ERROR.value, RDTorrentStatus.DEAD.value):
                    return DownloadCachedStreamResult(
                        success=False,
                        error=f"❌ Download failed with status: {status}"
                    )

                else:
                    logger.warning(f"⚠️ Unhandled status: {status}")
                    last_check_time = current_time
                    continue

            except Exception as e:
                logger.error(f"❌ Error checking download status: {str(e)}")
                return DownloadCachedStreamResult(success=False, error=f"Error checking status: {str(e)}")

            time.sleep(self.STATUS_CHECK_INTERVAL)

    def _add_active_download(self, content_id: str, torrent_id: str):
        """Add a download to active downloads tracking."""
        self.active_downloads[content_id].add(torrent_id)
        logger.debug(f"➕ Added download {self._color_text(torrent_id, 'cyan')} to content {self._color_text(content_id, 'cyan')} tracking")

    def _remove_active_download(self, content_id: str, torrent_id: str):
        """Remove a download from active downloads tracking."""
        if content_id in self.active_downloads:
            self.active_downloads[content_id].discard(torrent_id)
            logger.debug(f"➖ Removed download {self._color_text(torrent_id, 'yellow')} from content {self._color_text(content_id, 'yellow')} tracking")
            if not self.active_downloads[content_id]:
                del self.active_downloads[content_id]
                logger.debug(f"Removed empty content {content_id} from tracking")

    def _mark_content_complete(self, content_id: str):
        """Mark a content as having completed download."""
        self.download_complete[content_id] = True
        logger.debug(f"✨ Marked content {self._color_text(content_id, 'green')} as complete")

    def _is_content_complete(self, content_id: str) -> bool:
        """Check if content has completed download."""
        is_complete = content_id in self.download_complete and self.download_complete[content_id]
        logger.debug(f"Content {content_id} complete status: {self._color_text(str(is_complete), 'green' if is_complete else 'yellow')}")
        return is_complete

    def validate(self) -> bool:
        """
        Validate Real-Debrid settings and premium status
        Required by DownloaderBase
        """
        if not self._validate_settings():
            return False

        return self._validate_premium()

    def _validate_settings(self) -> bool:
        """Validate configuration settings"""
        if not self.api:
            return False
        return True

    def _validate_premium(self) -> bool:
        """Validate premium status"""
        try:
            user_info = self.api.request_handler.execute(HttpMethod.GET, "user")
            if not user_info.get("premium"):
                logger.error("Premium membership required")
                return False

            expiration = user_info.get("expiration")
            if expiration:
                try:
                    # Real-Debrid returns ISO format date string
                    expiry_date = datetime.fromisoformat(expiration.replace('Z', '+00:00'))
                    days_left = (expiry_date - datetime.now(timezone.utc)).days
                    logger.info(f"Your account expires in {days_left} days.")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse expiration date: {e}")

            return True
        except Exception as e:
            logger.error(f"Failed to validate premium status: {e}")
            return False

    def get_instant_availability(self, infohashes: List[str]) -> Dict[str, list]:
        """
        Get instant availability for multiple infohashes
        Required by DownloaderBase
        Note: Returns all torrents as available to attempt download of everything
        """
        # Return all infohashes as available with a dummy file entry
        result = {}
        for infohash in infohashes:
            result[infohash] = [{
                "files": [{
                    "id": 1,
                    "path": "pending.mkv",
                    "bytes": 1000000000
                }]
            }]
        return result

    def add_torrent(self, stream_or_hash: Union[Stream, str]) -> TorrentAddResult:
        """Add a torrent to Real-Debrid and select files"""
        try:
            # Handle both Stream object and infohash string
            if isinstance(stream_or_hash, Stream):
                infohash = stream_or_hash.infohash
            else:
                infohash = stream_or_hash
                
            # Construct magnet link from infohash
            magnet = f"magnet:?xt=urn:btih:{infohash}"
            
            # Add the torrent
            result = self._add_magnet_or_torrent(magnet=magnet)
            if not result or not result.success:
                error_msg = "Failed to add magnet" if not result else result.error
                return TorrentAddResult(success=False, error=error_msg, torrent_id="", info={})

            # Get torrent info
            torrent_info = self._get_torrent_info(result.torrent_id)
            if not torrent_info:
                return TorrentAddResult(success=False, error="Failed to get torrent info",
                                      torrent_id=result.torrent_id, info={})

            # Select files
            file_ids = self._get_media_file_ids(torrent_info)
            if not file_ids:
                return TorrentAddResult(success=False, error="No valid media files found",
                                      torrent_id=result.torrent_id, info=torrent_info)

            if not self.select_files(result.torrent_id, file_ids):
                return TorrentAddResult(success=False, error="Failed to select files",
                                      torrent_id=result.torrent_id, info=torrent_info)

            return TorrentAddResult(success=True, error=None,
                                  torrent_id=result.torrent_id, info=torrent_info)

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error adding torrent: {error_msg}")
            return TorrentAddResult(success=False, error=error_msg,
                                  torrent_id="", info={})

    def _get_alternative_streams(self, item: MediaItem, current_stream: Stream, max_count: int = 4) -> List[Stream]:
        """Get alternative streams for the same content, excluding the current stream"""
        alternatives = []
        try:
            # Get all available streams for the item
            all_streams = item.streams
            if not all_streams:
                return []

            # Filter and sort by seeders
            for stream in all_streams:
                if stream.infohash != current_stream.infohash:
                    alternatives.append(stream)
                    
            # Sort by seeder count if available
            alternatives.sort(key=lambda x: getattr(x, 'seeders', 0), reverse=True)
            
            return alternatives[:max_count]
        except Exception as e:
            logger.error(f"Error getting alternative streams: {e}")
            return []

    def _add_magnet_or_torrent(self, magnet: Optional[str] = None, torrent: Optional[bytes] = None, attempt: int = 1) -> TorrentAddResult:
        """Add a magnet link or torrent file to Real-Debrid with rate limit handling."""
        max_attempts = 5
        base_backoff = 2  # Base delay in seconds
        
        try:
            # Check active torrents before adding
            active_count = self.api.request_handler.execute(HttpMethod.GET, "torrents/activeCount")
            if active_count["nb"] >= active_count["limit"]:
                logger.warning("⚠️ Active limit exceeded, cleaning up inactive torrents...")
                cleaned = self._cleanup_inactive_torrents()
                if cleaned == 0:
                    # Try more aggressive cleanup
                    logger.warning("No torrents cleaned up, trying aggressive cleanup...")
                    cleaned = self._cleanup_all_except(set())
                    if cleaned == 0:
                        logger.error("❌ Could not free up torrent slots")
                        return TorrentAddResult(success=False, error="Could not free up torrent slots", torrent_id="", info={})
                        
            # Add the magnet with rate limiting
            with self.rate_limiter:
                logger.debug(f"🧲 Adding magnet (attempt {attempt}/{max_attempts}): {magnet[:64]}...")
                response = self.api.request_handler.execute(
                    HttpMethod.POST,
                    "torrents/addMagnet",
                    data={"magnet": magnet}
                )
                
                torrent_id = response.get("id")
                if torrent_id:
                    logger.debug(f"✅ Successfully added torrent {torrent_id}")
                    return TorrentAddResult(success=True, error=None, torrent_id=torrent_id, info=response)
                else:
                    logger.error("❌ Empty response from Real-Debrid API")
                    return TorrentAddResult(success=False, error="Empty response from Real-Debrid API", torrent_id="", info={})
                    
        except Exception as e:
            backoff = min(base_backoff * (2 ** (attempt - 1)), 30)  # Cap at 30 seconds
            
            if "Active Limit Exceeded" in str(e):
                # Try cleanup and retry quickly
                logger.warning(f"⚠️ Active limit exceeded, retrying cleanup...")
                cleaned = self._cleanup_inactive_torrents()
                if cleaned > 0:
                    backoff = 1  # Retry quickly if we cleaned something
                    
            if attempt < max_attempts:
                logger.warning(f"⏳ Retrying in {backoff}s... (attempt {attempt}/{max_attempts})")
                time.sleep(backoff)
                return self._add_magnet_or_torrent(magnet=magnet, torrent=torrent, attempt=attempt + 1)
            else:
                logger.error(f"❌ Failed to add torrent after {max_attempts} attempts: {e}")
                return TorrentAddResult(success=False, error=f"Failed to add torrent after {max_attempts} attempts: {e}", torrent_id="", info={})
        
        return TorrentAddResult(success=False, error="Unknown error", torrent_id="", info={})

    def _calculate_timeout(self, info: dict, item: MediaItem) -> float:
        """Calculate download timeout based on file size and progress"""
        # Get file size in MB
        file_size_mb = info.get("bytes", 0) / (1024 * 1024)  # Convert to MB
        
        # Calculate size-based timeout (10 seconds per 50MB)
        size_based_timeout = (file_size_mb / 50) * self.TIMEOUT_PER_50MB
        
        # Calculate final timeout with base and max limits
        timeout = min(
            self.BASE_TIMEOUT + size_based_timeout,
            self.MAX_TIMEOUT
        )
        
        # Log timeout calculation details
        logger.debug(
            f"Timeout calculation:\n"
            f"  File size: {file_size_mb:.1f}MB\n"
            f"  Base timeout: {self.BASE_TIMEOUT}s\n"
            f"  Size-based addition: {size_based_timeout:.1f}s\n"
            f"  Final timeout: {timeout:.1f}s"
        )
        
        return timeout

    def _color_text(self, text: str, color: str) -> str:
        """Add ANSI color to text"""
        colors = {
            'red': '\033[91m',
            'green': '\033[92m',
            'yellow': '\033[93m',
            'blue': '\033[94m',
            'magenta': '\033[95m',
            'cyan': '\033[96m',
            'white': '\033[97m',
            'reset': '\033[0m'
        }
        return f"{colors.get(color, '')}{text}{colors['reset']}"

    def _format_status(self, status: str) -> str:
        """Format status with color"""
        status = status.upper()
        color_map = {
            'DOWNLOADED': 'green',
            'DOWNLOADING': 'blue',
            'QUEUED': 'yellow',
            'WAITING_FILES_SELECTION': 'cyan',
            'ERROR': 'red',
            'MAGNET_ERROR': 'red',
            'DEAD': 'red'
        }
        return self._color_text(status, color_map.get(status.lower(), 'white'))

    def _format_progress(self, progress: float, speed: float, seeders: int) -> str:
        """Format progress info with colors"""
        progress_color = 'green' if progress >= 90 else 'yellow' if progress >= 50 else 'blue'
        speed_color = 'green' if speed > 5 else 'yellow' if speed > 1 else 'blue'
        seeders_color = 'green' if seeders > 10 else 'yellow' if seeders > 0 else 'red'
        
        return (
            f"Progress: {self._color_text(f'{progress:.1f}%', progress_color)}, "
            f"Speed: {self._color_text(f'{speed:.2f}MB/s', speed_color)}, "
            f"Seeders: {self._color_text(str(seeders), seeders_color)}"
        )

    def _format_count(self, current: int, limit: int) -> str:
        """Format count with color based on how close to limit"""
        color = 'green' if current < limit * 0.7 else 'yellow' if current < limit * 0.9 else 'red'
        return self._color_text(f"{current}/{limit}", color)

    def _format_attempt(self, attempt: int, max_attempts: int) -> str:
        """Format attempt counter with color"""
        color = 'green' if attempt <= max_attempts/2 else 'yellow' if attempt <= max_attempts*0.8 else 'red'
        return self._color_text(f"attempt {attempt}/{max_attempts}", color)

    def _format_deletion(self, reason: str) -> str:
        """Format deletion message"""
        return self._color_text(f"🗑️ {reason}", 'red')

    def _cleanup_all_except(self, exclude_ids: Set[str] = None) -> bool:
        """Clean up all non-downloaded torrents except those in exclude_ids"""
        exclude_ids = exclude_ids or set()
        cleaned_ids = set()

        try:
            # Get all torrents with retries
            max_retries = 3
            retry_count = 0
            torrents = None

            while retry_count < max_retries:
                try:
                    torrents = self._api_request_with_backoff(
                        HttpMethod.GET,
                        "torrents",
                        max_attempts=5
                    )
                    if isinstance(torrents, list):
                        break
                    retry_count += 1
                    time.sleep(5)
                except Exception as e:
                    logger.warning(f"Failed to get torrents (attempt {retry_count + 1}/{max_retries}): {e}")
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(5)

            if not isinstance(torrents, list):
                logger.error(f"Invalid response from torrents endpoint after {max_retries} attempts")
                return False

            # First check active torrent count
            try:
                active_count = self.api.request_handler.execute(HttpMethod.GET, "torrents/activeCount")
                logger.debug(f"⚠️ At active torrent limit {self._format_count(active_count['nb'], active_count['limit'])}")
                if active_count["nb"] < active_count["limit"]:
                    return 0
                
                # Calculate how aggressive we should be based on how far over the limit we are
                overage = active_count["nb"] - active_count["limit"]
                logger.warning(f"Over active torrent limit by {overage} torrents")
                # If we're over by more than 5, be extremely aggressive
                extremely_aggressive = overage >= 5
                # If we're over by any amount, be somewhat aggressive
                aggressive_cleanup = overage > 0
            except Exception as e:
                logger.warning(f"Failed to get active torrent count: {e}")
                extremely_aggressive = True  # Be extremely aggressive if we can't check
                aggressive_cleanup = True
            
            # Get list of all torrents
            torrents = self.api.request_handler.execute(HttpMethod.GET, "torrents")
            to_delete = []  # List of (priority, torrent_id, reason) tuples
            cleaned = 0
            
            # Count active torrents by status and collect stats
            active_by_status = defaultdict(list)
            magnet_times = []  # Track magnet conversion times
            downloading_stats = []  # Track download stats
            total_active = 0
            
            # Track duplicates by filename
            filename_to_torrents = defaultdict(list)
            
            for torrent in torrents:
                status = torrent.get("status", "")
                if self._is_active_status(status):
                    # Calculate time_elapsed first
                    time_elapsed = 0
                    try:
                        added = torrent.get("added", "")
                        if added:
                            # Convert to UTC, then to local time
                            added_time = datetime.fromisoformat(added.replace("Z", "+00:00"))
                            added_time = added_time.astimezone().replace(tzinfo=None)
                            time_elapsed = (datetime.now() - added_time).total_seconds()
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid timestamp format for torrent: {torrent.get('added')}")
                    
                    torrent_stats = {
                        "status": status,
                        "filename": torrent.get("filename", "unknown"),
                        "progress": torrent.get("progress", 0),
                        "speed": torrent.get("speed", 0),
                        "seeders": torrent.get("seeders", 0),
                        "time_elapsed": time_elapsed,
                        "id": torrent.get("id", "")
                    }
                    
                    active_by_status[status].append(torrent_stats)
                    filename_to_torrents[torrent_stats["filename"]].append(torrent_stats)
                    total_active += 1
                    
                    if status == "magnet_conversion" and time_elapsed > 0:
                        magnet_times.append(time_elapsed)
                    elif status == "downloading":
                        downloading_stats.append(torrent_stats)
            
            # First handle duplicates - keep only the most progressed version of each file
            for filename, dupes in filename_to_torrents.items():
                if len(dupes) > 1:
                    logger.info(f"Found {len(dupes)} duplicates of {filename}")
                    # Sort by progress (highest first), then by speed (highest first)
                    dupes.sort(key=lambda x: (x["progress"], x["speed"]), reverse=True)
                    
                    # Keep the best one, mark others for deletion
                    best = dupes[0]
                    logger.info(f"Keeping best duplicate: {best['progress']}% @ {best['speed']/1024:.1f} KB/s")
                    
                    for dupe in dupes[1:]:
                        reason = (f"duplicate of {filename} "
                                f"(keeping: {best['progress']}% @ {best['speed']/1024:.1f} KB/s, "
                                f"removing: {dupe['progress']}% @ {dupe['speed']/1024:.1f} KB/s)")
                        to_delete.append((150, dupe["id"], reason, dupe["time_elapsed"]))  # Highest priority for duplicates
                        logger.info(f"Marking duplicate for deletion: {reason}")
            
            # Find stalled or problematic torrents
            stalled_threshold = 60  # 1 minute without progress
            near_complete_threshold = 95.0  # Protect torrents above this %
            min_speed_threshold = 100 * 1024  # 100 KB/s minimum speed
            
            for status, torrents in active_by_status.items():
                for t in torrents:
                    # Skip nearly complete downloads unless they're completely stalled
                    if t["progress"] >= near_complete_threshold:
                        if t["speed"] == 0:
                            logger.warning(f"Nearly complete torrent stalled: {t['filename']} at {t['progress']}%")
                            reason = f"stalled at {t['progress']}% complete (no speed for {t['time_elapsed']/60:.1f}m)"
                            to_delete.append((90, t["id"], reason, t["time_elapsed"]))
                        continue
                    
                    # Check for stalled downloads
                    if status == "downloading":
                        if t["speed"] < min_speed_threshold:
                            time_stalled = t["time_elapsed"]
                            if time_stalled > stalled_threshold:
                                reason = (f"stalled download: {t['filename']} "
                                        f"(progress: {t['progress']}%, "
                                        f"speed: {t['speed']/1024:.1f} KB/s, "
                                        f"stalled for: {time_stalled/60:.1f}m)")
                                priority = 120 if t["progress"] < 10 else 100  # Higher priority for early stalls
                                to_delete.append((priority, t["id"], reason, time_stalled))
                                logger.info(f"Marking stalled download for deletion: {reason}")
                    
                    # Handle stuck magnet conversions more aggressively
                    elif status == "magnet_conversion":
                        if t["time_elapsed"] > 300:  # 5 minutes
                            reason = f"stuck in magnet conversion for {t['time_elapsed']/60:.1f} minutes"
                            to_delete.append((130, t["id"], reason, t["time_elapsed"]))
                            logger.info(f"Marking stuck magnet for deletion: {reason}")
            
            # Log active torrent distribution and detailed stats
            logger.info("=== Active Torrent Stats ===")
            for status, active_torrents in active_by_status.items():
                count = len(active_torrents)
                logger.info(f"\n{status.upper()} ({count} torrents):")
                
                # Sort by time elapsed
                active_torrents.sort(key=lambda x: x["time_elapsed"], reverse=True)
                
                for t in active_torrents:
                    stats = []
                    if t["progress"] > 0:
                        stats.append(f"progress: {t['progress']}%")
                    if t["speed"] > 0:
                        stats.append(f"speed: {t['speed']/1024:.1f} KB/s")
                    if t["seeders"] > 0:
                        stats.append(f"seeders: {t['seeders']}")
                    if t["time_elapsed"] > 0:
                        stats.append(f"age: {t['time_elapsed']/60:.1f}m")
                    
                    stats_str = ", ".join(stats) if stats else f"age: {t['time_elapsed']/60:.1f}m"
                    logger.info(f"  - {t['filename']} ({stats_str})")
            
            # Calculate duplicate ratio and adjust aggressiveness
            unique_filenames = set()
            for status, torrents in active_by_status.items():
                for t in torrents:
                    unique_filenames.add(t["filename"])
            
            duplicate_ratio = (total_active - len(unique_filenames)) / total_active if total_active > 0 else 0
            if duplicate_ratio > 0.5:  # If more than 50% are duplicates
                extremely_aggressive = True
                logger.info(f"High duplicate ratio ({duplicate_ratio:.1%}), using extremely aggressive cleanup")
            
            # Set base thresholds
            if extremely_aggressive:
                magnet_threshold = 30  # 30 seconds
                time_threshold = self.CLEANUP_INACTIVE_TIME / 4
            elif aggressive_cleanup:
                magnet_threshold = 60  # 1 minute
                time_threshold = self.CLEANUP_INACTIVE_TIME / 2
            else:
                magnet_threshold = 300  # 5 minutes
                time_threshold = self.CLEANUP_INACTIVE_TIME
            
            logger.debug(f"⏱️ Using thresholds - Magnet: {self._color_text(f'{magnet_threshold/60:.1f}m', 'cyan')}, General: {self._color_text(f'{time_threshold/60:.1f}m', 'cyan')}")
            
            # Process all torrents for cleanup
            for status, torrents in active_by_status.items():
                for torrent_stats in torrents:
                    should_delete = False
                    reason = ""
                    priority = 0
                    time_elapsed = torrent_stats["time_elapsed"]
                    
                    # 1. Error states (highest priority)
                    if status in ("error", "magnet_error", "virus", "dead"):
                        should_delete = True
                        reason = f"error status: {status}"
                        priority = 100
                    
                    # 2. Magnet conversion (high priority if taking too long)
                    elif status == "magnet_conversion":
                        if time_elapsed > magnet_threshold:
                            should_delete = True
                            reason = f"stuck in magnet conversion for {time_elapsed/60:.1f} minutes"
                            priority = 95  # Very high priority since we have so many
                    
                    # 3. Stalled or slow downloads
                    elif status == "downloading":
                        progress = torrent_stats["progress"]
                        speed = torrent_stats["speed"]
                        seeders = torrent_stats["seeders"]
                        
                        if progress == 0 and time_elapsed > time_threshold:
                            should_delete = True
                            reason = f"no progress after {time_elapsed/60:.1f} minutes"
                            priority = 85
                        elif progress < self.CLEANUP_MINIMAL_PROGRESS_THRESHOLD and time_elapsed > time_threshold:
                            should_delete = True
                            reason = f"minimal progress ({progress}%) after {time_elapsed/60:.1f} minutes"
                            priority = 80
                        elif speed < self.CLEANUP_SPEED_THRESHOLD:
                            should_delete = True
                            reason = f"slow speed ({speed/1024:.1f} KB/s)"
                            priority = 75
                        elif seeders == 0:
                            should_delete = True
                            reason = f"no seeders"
                            priority = 85
                    
                    # 4. Stuck uploads/compression
                    elif status in ("uploading", "compressing"):
                        speed = torrent_stats["speed"]
                        if time_elapsed > time_threshold or speed < self.CLEANUP_SPEED_THRESHOLD:
                            should_delete = True
                            reason = f"stuck in {status} for {time_elapsed/60:.1f} minutes"
                            priority = 60
                    
                    # 5. Other states
                    elif status in ("waiting_files_selection", "queued"):
                        if time_elapsed > time_threshold:
                            should_delete = True
                            reason = f"stuck in {status} for {time_elapsed/60:.1f} minutes"
                            priority = 50
                    
                    if should_delete:
                        filename = torrent_stats["filename"]
                        progress = torrent_stats["progress"]
                        speed = torrent_stats["speed"]
                        full_reason = f"{reason} (file: {filename}, progress: {progress}%, speed: {speed/1024:.1f} KB/s)"
                        to_delete.append((priority, torrent_stats["id"], full_reason, time_elapsed))
            
            # Sort by priority (highest first) and extract torrent_id and reason
            to_delete.sort(reverse=True)
            
            # If we're extremely aggressive, take more torrents
            batch_size = self.CLEANUP_BATCH_SIZE * 2 if extremely_aggressive else self.CLEANUP_BATCH_SIZE
            
            # If no torrents were marked for deletion but we're still over limit,
            # force delete the slowest/least progressed torrents
            if not to_delete and total_active > active_count["limit"]:
                logger.info("No torrents met deletion criteria but still over limit, using fallback cleanup")
                
                # First try to clean up just duplicates
                duplicates_only = True
                cleanup_attempts = 2  # Try duplicates first, then all torrents if needed
                
                while cleanup_attempts > 0:
                    # Collect all active torrents into a single list for sorting
                    all_active = []
                    seen_filenames = set()
                    
                    for status, torrents in active_by_status.items():
                        for t in torrents:
                            filename = t["filename"]
                            
                            # Skip non-duplicates on first pass
                            is_duplicate = filename in seen_filenames
                            if duplicates_only and not is_duplicate:
                                continue
                            
                            seen_filenames.add(filename)
                            
                            score = 0
                            # Prioritize keeping torrents with more progress
                            score += t["progress"] * 100
                            # And those with higher speeds
                            score += min(t["speed"] / 1024, 1000)  # Cap speed bonus at 1000
                            # And those with more seeders
                            score += t["seeders"] * 10
                            # Penalize older torrents slightly
                            score -= min(t["time_elapsed"] / 60, 60)  # Cap age penalty at 60 minutes
                            # Heavy penalty for duplicates
                            if is_duplicate:
                                score -= 5000  # Ensure duplicates are cleaned up first
                            
                            all_active.append({
                                "id": t["id"],
                                "score": score,
                                "stats": t,
                                "status": status,
                                "is_duplicate": is_duplicate
                            })
                    
                    if all_active:
                        # Sort by score (lowest first - these will be deleted)
                        all_active.sort(key=lambda x: x["score"])
                        
                        # Take enough torrents to get under the limit
                        to_remove = min(
                            len(all_active),  # Don't try to remove more than we have
                            total_active - active_count["limit"] + 1  # +1 for safety margin
                        )
                        
                        for torrent in all_active[:to_remove]:
                            stats = torrent["stats"]
                            reason = (f"fallback cleanup{' (duplicate)' if duplicates_only else ''} - {torrent['status']} "
                                    f"(progress: {stats['progress']}%, "
                                    f"speed: {stats['speed']/1024:.1f} KB/s, "
                                    f"seeders: {stats['seeders']}, "
                                    f"age: {stats['time_elapsed']/60:.1f}m)")
                            to_delete.append((0, torrent["id"], reason, stats["time_elapsed"]))
                            logger.info(f"Fallback cleanup marking: {stats['filename']} - {reason}")
                        
                        # If we found enough torrents to delete, we're done
                        if len(to_delete) >= (total_active - active_count["limit"]):
                            break
                    
                    # If we get here and duplicates_only is True, try again with all torrents
                    duplicates_only = False
                    cleanup_attempts -= 1
                
                # Log what we're about to delete
                if to_delete:
                    logger.info(f"Found {len(to_delete)} torrents to clean up, processing in batches of {batch_size}")
                    for _, _, reason, _ in to_delete[:5]:  # Log first 5 for debugging
                        logger.debug(f"Will delete: {reason}")
            
            # Convert to final format
            to_delete = [(t[1], t[2]) for t in to_delete]
            
            # Process deletion in batches
            while to_delete:
                batch = to_delete[:batch_size]
                to_delete = to_delete[batch_size:]
                cleaned += self._batch_delete_torrents(batch)
            
            # Update last cleanup time if any torrents were cleaned
            if cleaned > 0:
                self.last_cleanup_time = datetime.now()
                logger.info(f"Cleaned up {cleaned} torrents")
            else:
                logger.warning("No torrents were cleaned up despite being over the limit!")
            
            return cleaned
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return 0

    def _batch_delete_torrents(self, torrents: List[Tuple[str, str]]) -> int:
        """Delete a batch of torrents efficiently.
        Args:
            torrents: List of (torrent_id, reason) tuples
        Returns:
            Number of successfully deleted torrents
        """
        deleted = 0
        for torrent_id, reason in torrents:
            try:
                # First try to delete associated downloads
                try:
                    downloads = self.api.request_handler.execute(HttpMethod.GET, "downloads")
                    for download in downloads:
                        if download.get("torrent_id") == torrent_id:
                            try:
                                self.api.request_handler.execute(HttpMethod.DELETE, f"downloads/delete/{download['id']}")
                                logger.debug(f"Deleted download {download['id']} associated with torrent {torrent_id}")
                            except Exception as e:
                                logger.warning(f"Failed to delete download {download['id']}: {e}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup downloads for torrent {torrent_id}: {e}")

                # Then delete the torrent
                self.api.request_handler.execute(HttpMethod.DELETE, f"torrents/delete/{torrent_id}")
                logger.info(f"Deleted torrent {torrent_id}: {reason}")
                deleted += 1
            except Exception as e:
                if "404" in str(e):
                    # Torrent was already deleted, count it as success
                    logger.debug(f"Torrent {torrent_id} was already deleted")
                    deleted += 1
                elif "401" in str(e):
                    logger.error("API token expired or invalid")
                    break  # Stop processing batch
                elif "403" in str(e):
                    logger.error("Account locked or permission denied")
                    break  # Stop processing batch
                else:
                    logger.error(f"Failed to delete torrent {torrent_id}: {e}")
        return deleted

    def _get_torrent_info(self, torrent_id: str) -> Optional[Dict]:
        """Get detailed info for a torrent including seeders and status"""
        try:
            logger.debug(f"📡 Fetching info for torrent {self._color_text(torrent_id, 'cyan')}")
            info = self.api.request_handler.execute(HttpMethod.GET, f"torrents/info/{torrent_id}")
            
            if not info:
                logger.error(f"❌ Empty response for torrent {self._color_text(torrent_id, 'red')}")
                return None
                
            # Log detailed torrent info
            status = info.get('status', 'unknown')
            filename = info.get('filename', 'unknown')
            bytes_size = info.get('bytes', 0)
            progress = info.get('progress', 0)
            speed = info.get('speed', 0)
            seeders = info.get('seeders', 0)
            
            logger.debug(
                f"📊 Torrent info for {self._color_text(torrent_id, 'cyan')}:\n"
                f"  • Name: {filename}\n"
                f"  • Status: {self._format_status(status)}\n"
                f"  • Size: {self._format_size(bytes_size)}\n"
                f"  • Progress: {self._format_progress(progress, speed, seeders)}\n"
                f"  • Files: {len(info.get('files', []))}"
            )
            
            return info
            
        except Exception as e:
            logger.error(f"❌ Error getting torrent info for {self._color_text(torrent_id, 'red')}: {str(e)}")
            return None

    def _handle_rate_limit(self, attempt: int) -> float:
        """Handle rate limit with exponential backoff.
        Returns the delay time in seconds."""
        if attempt >= self.RATE_LIMIT_MAX_RETRIES:
            raise Exception(f"Rate limit retry count exceeded ({self.RATE_LIMIT_MAX_RETRIES})")
            
        delay = min(
            self.RATE_LIMIT_INITIAL_DELAY * (self.RATE_LIMIT_BACKOFF_BASE ** attempt),
            self.RATE_LIMIT_MAX_DELAY
        )
        logger.warning(f"⏳ Rate limited, waiting {delay:.1f}s (attempt {attempt + 1}/{self.RATE_LIMIT_MAX_RETRIES})")
        return delay

    def _api_request_with_backoff(self, method: HttpMethod, endpoint: str, data: Optional[Dict] = None, 
                                files: Optional[Dict] = None, attempt: int = 0) -> Any:
        """Make API request with rate limit handling and exponential backoff."""
        try:
            # Wait for rate limiter before making request
            self.api.rate_limiter.acquire()
            
            # Build kwargs dict for the request
            kwargs = {}
            if data:
                kwargs.update(data)
            if files:
                kwargs.update(files)
            
            # Execute request with single kwargs dict
            response = self.api.request_handler.execute(method, endpoint, kwargs)
            
            if not isinstance(response, (dict, list)) and response is not None:
                logger.warning(f"Unexpected response type from API: {type(response)}")
                
            return response
            
        except Exception as e:
            error_str = str(e)
            
            # Handle rate limit (509)
            if "509" in error_str and "Active Limit Exceeded" in error_str:
                if attempt >= self.RATE_LIMIT_MAX_RETRIES:
                    raise Exception(f"Rate limit retry count exceeded ({self.RATE_LIMIT_MAX_RETRIES})")
                    
                delay = self._handle_rate_limit(attempt)
                time.sleep(delay)
                
                # Recursive retry with incremented attempt count
                return self._api_request_with_backoff(method, endpoint, data, files, attempt + 1)
            
            # Other API errors
            raise

    def get_torrent_info(self, torrent_id: str) -> dict:
        """
        Get information about a torrent
        Required by DownloaderBase
        """
        if not self.initialized:
            raise RealDebridError("Downloader not properly initialized")

        response = self.api.request_handler.execute(
            HttpMethod.GET,
            f"torrents/info/{torrent_id}"
        )
        
        # Log a cleaner version with just the important info
        if response:
            status = response.get('status', 'unknown')
            progress = response.get('progress', 0)
            speed = response.get('speed', 0)
            seeders = response.get('seeders', 0)
            filename = response.get('filename', 'unknown')
            files = response.get('files', [])
            
            speed_mb = speed / 1000000 if speed else 0  # Convert to MB/s
            
            logger.debug(
                f"📊 {self._format_progress(progress, speed_mb, seeders)}\n"
                f"🎬 Status: {self._format_status(status)}"
            )
            
            # Log file details if available
            if files:
                logger.debug("📁 Available files:")
                for f in files:
                    logger.debug(f"- {f.get('path', 'unknown')} ({f.get('bytes', 0)} bytes)")
        
        return response

    def delete_torrent(self, torrent_id: str):
        """
        Delete a torrent
        Required by DownloaderBase
        """
        if not self.initialized:
            raise RealDebridError("Downloader not properly initialized")

        try:
            self.api.request_handler.execute(
                HttpMethod.DELETE,
                f"torrents/delete/{torrent_id}"
            )
        except Exception as e:
            error_str = str(e)
            if "404" in error_str:
                # Could mean: already deleted, invalid ID, or never existed
                logger.warning(f"Could not delete torrent {torrent_id}: Unknown resource (404)")
                return
            elif "401" in str(e):
                logger.error("API token expired or invalid")
                raise
            elif "403" in str(e):
                logger.error("Account locked or permission denied")
                raise
            else:
                logger.error(f"Failed to delete torrent {torrent_id}: {error_str}")
                raise

    def _can_start_download(self, content_id: str) -> bool:
        """Check if we can start a new download for this content."""
        with self._lock:
            # Check if this content is already complete
            if self._is_content_complete(content_id):
                logger.debug(f"Content {content_id} already complete")
                return False

            # Get total unique content IDs currently downloading
            active_content_ids = set(self.active_downloads.keys())
            if len(active_content_ids) >= self.MAX_CONCURRENT_CONTENT and content_id not in active_content_ids:
                logger.warning(f"⚠️ Too many content items downloading ({len(active_content_ids)}/{self.MAX_CONCURRENT_CONTENT})")
                return False

            # Check per-content limit
            active_for_content = len(self.active_downloads.get(content_id, set()))
            if active_for_content >= self.MAX_CONCURRENT_PER_CONTENT:
                logger.warning(f"⚠️ Too many downloads for content {content_id} ({active_for_content}/{self.MAX_CONCURRENT_PER_CONTENT})")
                return False

            # Check total concurrent limit
            total_active = sum(len(downloads) for downloads in self.active_downloads.values())
            if total_active >= self.MAX_CONCURRENT_TOTAL:
                logger.warning(f"⚠️ Too many total concurrent downloads ({total_active}/{self.MAX_CONCURRENT_TOTAL})")
                return False

            return True

    def _is_active_status(self, status: str) -> bool:
        """Check if a torrent status counts as active."""
        return status in ("downloading", "uploading", "compressing", "magnet_conversion", "waiting_files_selection")

    def _cleanup_inactive_torrents(self) -> int:
        """Clean up inactive, errored, or stalled torrents to free up slots.
        Returns number of torrents cleaned up."""
        
        current_time = time.time()
        if (current_time - self.last_cleanup_time) < self.cleanup_interval:
            return 0
            
        try:
            # Get active torrent count
            try:
                active_count = self.api.request_handler.execute(HttpMethod.GET, "torrents/activeCount")
                logger.debug(f"⚠️ At active torrent limit {self._format_count(active_count['nb'], active_count['limit'])}")
                if active_count["nb"] < active_count["limit"]:
                    return 0
                
                # Calculate how aggressive we should be based on how far over the limit we are
                overage = active_count["nb"] - active_count["limit"]
                logger.warning(f"Over active torrent limit by {overage} torrents")
                # If we're over by more than 5, be extremely aggressive
                extremely_aggressive = overage >= 5
                # If we're over by any amount, be somewhat aggressive
                aggressive_cleanup = overage > 0
            except Exception as e:
                logger.warning(f"Failed to get active torrent count: {e}")
                extremely_aggressive = True  # Be extremely aggressive if we can't check
                aggressive_cleanup = True
            
            # Get list of all torrents
            torrents = self.api.request_handler.execute(HttpMethod.GET, "torrents")
            to_delete = []  # List of (priority, torrent_id, reason) tuples
            cleaned = 0
            
            # Count active torrents by status and collect stats
            active_by_status = defaultdict(list)
            magnet_times = []  # Track magnet conversion times
            downloading_stats = []  # Track download stats
            total_active = 0
            
            # Track duplicates by filename
            filename_to_torrents = defaultdict(list)
            
            for torrent in torrents:
                status = torrent.get("status", "")
                if not self._is_active_status(status):
                    continue
                    
                # Calculate elapsed time
                time_elapsed = 0
                try:
                    added = torrent.get("added", "")
                    if added:
                        # Convert to UTC, then to local time
                        added_time = datetime.fromisoformat(added.replace("Z", "+00:00"))
                        added_time = added_time.astimezone().replace(tzinfo=None)
                        time_elapsed = (datetime.now() - added_time).total_seconds()
                except (ValueError, TypeError):
                    logger.warning(f"Invalid timestamp format for torrent: {torrent.get('added')}")
                    
                torrent_stats = {
                    "status": status,
                    "filename": torrent.get("filename", "unknown"),
                    "progress": torrent.get("progress", 0),
                    "speed": torrent.get("speed", 0),
                    "seeders": torrent.get("seeders", 0),
                    "time_elapsed": time_elapsed,
                    "id": torrent.get("id", "")
                }
                
                # Track potential duplicates
                filename_to_torrents[torrent_stats["filename"]].append(torrent_stats)
                
                # Check for stalled downloads
                if status == "downloading":
                    # Only consider a download stalled if it has no seeders and no progress
                    if torrent_stats["speed"] == 0 and torrent_stats["seeders"] == 0:
                        if time_elapsed > 300:  # 5 minutes
                            reason = f"stalled with no seeders (file: {torrent_stats['filename']}, progress: {torrent_stats['progress']}%)"
                            to_delete.append((90, torrent_stats["id"], reason))
                    # Or if it's moving extremely slowly
                    elif torrent_stats["speed"] < self.CLEANUP_SPEED_THRESHOLD:
                        if time_elapsed > 900:  # 15 minutes
                            speed_kb = torrent_stats["speed"] / 1024
                            reason = f"extremely slow ({speed_kb:.1f} KB/s) (file: {torrent_stats['filename']}, progress: {torrent_stats['progress']}%)"
                            to_delete.append((80, torrent_stats["id"], reason))
                
                # Check for stuck magnet conversions
                elif status == "magnet_conversion" and time_elapsed > 600:  # 10 minutes
                    reason = f"stuck in magnet conversion for {time_elapsed:.0f}s"
                    to_delete.append((70, torrent_stats["id"], reason))
        
            # Handle duplicates - only if we're being aggressive
            if aggressive_cleanup:
                for filename, dupes in filename_to_torrents.items():
                    if len(dupes) > 1:
                        # Sort by progress (highest first), then by speed, then by seeders
                        dupes.sort(key=lambda x: (x["progress"], x["speed"], x["seeders"]), reverse=True)
                        best = dupes[0]
                        
                        # Only remove duplicates if they're significantly worse
                        for dupe in dupes[1:]:
                            # Keep both if they're both making good progress
                            if dupe["progress"] > 20 and dupe["speed"] > self.CLEANUP_SPEED_THRESHOLD:
                                continue
                                
                            reason = (f"duplicate of {filename} "
                                    f"(keeping: {best['progress']}% @ {best['speed']/1024:.1f} KB/s with {best['seeders']} seeders, "
                                    f"removing: {dupe['progress']}% @ {dupe['speed']/1024:.1f} KB/s with {dupe['seeders']} seeders)")
                            to_delete.append((60, dupe["id"], reason))
        
            # Sort by priority and delete
            to_delete.sort(reverse=True)
            
            # Determine how many torrents to delete
            delete_count = len(to_delete)
            if extremely_aggressive:
                delete_count = max(delete_count, min(5, len(torrents)))
            elif aggressive_cleanup:
                delete_count = max(delete_count, min(3, len(torrents)))
            
            # Delete torrents
            for _, torrent_id, reason in to_delete[:delete_count]:
                try:
                    self.api.request_handler.execute(HttpMethod.DELETE, f"torrents/delete/{torrent_id}")
                    cleaned += 1
                    logger.info(f"Cleaned up torrent: {reason}")
                except Exception as e:
                    logger.error(f"Failed to delete torrent {torrent_id}: {e}")
        
            self.last_cleanup_time = datetime.now()
            if cleaned:
                logger.info(f"🧹 Cleaned up {cleaned} inactive torrents")
            return cleaned
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return 0

    def download_cached_stream(self, item: MediaItem, stream: Stream) -> DownloadCachedStreamResult:
        """Download a stream from Real-Debrid with sequential parallel checking"""
        content_id = item.id if item else None
        
        # Check if content is already complete
        if self._is_content_complete(content_id):
            logger.debug(f"Content {content_id} already complete")
            return DownloadCachedStreamResult(success=True, error=None, torrent_id="", info={})
        
        # Get alternative streams to try if the main one fails
        alternative_streams = self._get_alternative_streams(item, stream)
        all_streams = [stream] + alternative_streams
        
        # Try each stream in sequence
        for i, current_stream in enumerate(all_streams):
            if i > 0:
                logger.debug(f"📥 Trying alternative torrent {i}/{len(alternative_streams)}...")
                
            # Check if we can start a new download
            if not self._can_start_download(content_id):
                logger.warning(f"Too many active downloads for {content_id}")
                return DownloadCachedStreamResult(success=False, error=f"Too many active downloads for {content_id}")
                
            try:
                # Try to add and process the torrent
                if self._try_download_stream(current_stream, item):
                    return DownloadCachedStreamResult(success=True, error=None, torrent_id="", info={})
                    
            except Exception as e:
                logger.error(f"Error downloading stream {current_stream.id}: {e}")
                continue
                
        return DownloadCachedStreamResult(success=False, error="No torrents downloaded successfully")

    def _try_download_stream(self, stream: Stream, item: MediaItem) -> bool:
        """Try to download a stream"""
        # Construct magnet link from infohash
        magnet = f"magnet:?xt=urn:btih:{stream.infohash}"
        result = self._add_magnet_or_torrent(magnet=magnet)
        if not result:
            return False
        
        # Get torrent info
        torrent_info = self._get_torrent_info(result.torrent_id)
        if not torrent_info:
            return False
        
        # Select files
        file_ids = self._get_media_file_ids(torrent_info)
        if not file_ids:
            return False
        
        # Download the torrent
        return self.wait_for_download(result.torrent_id, item.id, item, stream)

    def _add_magnet_or_torrent(self, magnet: Optional[str] = None, torrent: Optional[bytes] = None, attempt: int = 1) -> TorrentAddResult:
        """Add a magnet or torrent to Real-Debrid with rate limit handling."""
        max_attempts = 5
        base_backoff = 2  # Base delay in seconds
        
        try:
            # Check active torrents before adding
            active_count = self.api.request_handler.execute(HttpMethod.GET, "torrents/activeCount")
            if active_count["nb"] >= active_count["limit"]:
                logger.warning("⚠️ Active limit exceeded, cleaning up inactive torrents...")
                cleaned = self._cleanup_inactive_torrents()
                if cleaned == 0:
                    # Try more aggressive cleanup
                    logger.warning("No torrents cleaned up, trying aggressive cleanup...")
                    cleaned = self._cleanup_all_except(set())
                    if cleaned == 0:
                        logger.error("❌ Could not free up torrent slots")
                        return TorrentAddResult(success=False, error="Could not free up torrent slots", torrent_id="", info={})
                        
            # Add the magnet with rate limiting
            with self.rate_limiter:
                logger.debug(f"🧲 Adding magnet (attempt {attempt}/{max_attempts}): {magnet[:64]}...")
                response = self.api.request_handler.execute(
                    HttpMethod.POST,
                    "torrents/addMagnet",
                    data={"magnet": magnet}
                )
                
                torrent_id = response.get("id")
                if torrent_id:
                    logger.debug(f"✅ Successfully added torrent {torrent_id}")
                    return TorrentAddResult(success=True, error=None, torrent_id=torrent_id, info=response)
                else:
                    logger.error("❌ Empty response from Real-Debrid API")
                    return TorrentAddResult(success=False, error="Empty response from Real-Debrid API", torrent_id="", info={})
                    
        except Exception as e:
            backoff = min(base_backoff * (2 ** (attempt - 1)), 30)  # Cap at 30 seconds
            
            if "Active Limit Exceeded" in str(e):
                # Try cleanup and retry quickly
                logger.warning(f"⚠️ Active limit exceeded, retrying cleanup...")
                cleaned = self._cleanup_inactive_torrents()
                if cleaned > 0:
                    backoff = 1  # Retry quickly if we cleaned something
                    
            if attempt < max_attempts:
                logger.warning(f"⏳ Retrying in {backoff}s... (attempt {attempt}/{max_attempts})")
                time.sleep(backoff)
                return self._add_magnet_or_torrent(magnet=magnet, torrent=torrent, attempt=attempt + 1)
            else:
                logger.error(f"❌ Failed to add torrent after {max_attempts} attempts: {e}")
                return TorrentAddResult(success=False, error=f"Failed to add torrent after {max_attempts} attempts: {e}", torrent_id="", info={})