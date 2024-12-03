import time
import threading
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, Union, Any
from collections import defaultdict
from datetime import timedelta
import random
from requests import Response

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

from .download_manager import DownloadManager

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
    """Rate limiter for Real-Debrid API with exponential backoff."""
    
    def __init__(self, max_requests: int = 90, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self._lock = threading.Lock()
        self.backoff_until = 0
        self.consecutive_errors = 0
        
    def __enter__(self):
        self.acquire()
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            if "rate limit exceeded" in str(exc_val).lower():
                self.handle_error()
        
    def acquire(self):
        """Acquire permission to make a request."""
        with self._lock:
            current_time = time.time()
            
            # First check if we're in backoff
            if current_time < self.backoff_until:
                wait_time = self.backoff_until - current_time
                logger.debug(f"Still in backoff period, waiting {wait_time:.1f}s")
                time.sleep(wait_time)
            
            # Clean old requests
            self.requests = [t for t in self.requests if current_time - t < self.time_window]
            
            # Check if we can make a request
            if len(self.requests) >= self.max_requests:
                oldest_request = min(self.requests)
                wait_time = oldest_request + self.time_window - current_time
                if wait_time > 0:
                    logger.debug(f"Rate limit reached, waiting {wait_time:.1f}s")
                    time.sleep(wait_time)
                    # Clean old requests again after waiting
                    current_time = time.time()
                    self.requests = [t for t in self.requests if current_time - t < self.time_window]
            
            # Add the new request
            self.requests.append(current_time)
            logger.debug(f"✅ Rate limit request approved ({len(self.requests)}/{self.max_requests})")
            
    def handle_error(self):
        """Handle a rate limit error with exponential backoff."""
        with self._lock:
            self.consecutive_errors += 1
            backoff_time = min(2 ** self.consecutive_errors, 30)  # Cap at 30 seconds
            self.backoff_until = time.time() + backoff_time
            logger.warning(f"⚠️ Rate limit error, backing off for {backoff_time:.1f}s")
            
    def reset(self):
        """Reset the rate limiter state."""
        with self._lock:
            self.requests = []
            self.consecutive_errors = 0
            self.backoff_until = 0

class RealDebridAPI:
    """Real-Debrid API client with rate limiting and error handling."""
    
    # API endpoints
    ENDPOINTS = {
        'torrents': '/torrents',  # GET - Get user torrents list
        'torrent_info': '/torrents/info',  # GET /{id} - Get info on torrent
        'torrents_active': '/torrents/activeCount',  # GET - Get currently active torrents
        'torrents_hosts': '/torrents/availableHosts',  # GET - Get available hosts
        'torrent_add': '/torrents/addTorrent',  # PUT - Add torrent file
        'magnet_add': '/torrents/addMagnet',  # POST - Add magnet link
        'torrent_select': '/torrents/selectFiles',  # POST /{id} - Select files
        'torrent_delete': '/torrents/delete',  # DELETE /{id} - Delete torrent
        'downloads': '/downloads',  # GET - Get user downloads list
        'download_delete': '/downloads/delete',  # DELETE /{id} - Delete download
        'unrestrict_check': '/unrestrict/check',  # POST - Check a link
        'unrestrict_link': '/unrestrict/link',  # POST - Unrestrict a link
        'unrestrict_folder': '/unrestrict/folder',  # POST - Unrestrict a folder link
        'unrestrict_container_file': '/unrestrict/containerFile',  # PUT - Decrypt container file
        'unrestrict_container_link': '/unrestrict/containerLink'  # POST - Decrypt container from link
    }
    
    BATCH_SIZE = 10  # Number of items to process in a batch
    BATCH_DELAY = 2  # Delay between batches in seconds
    
    def __init__(self, api_key: str, proxy_url: Optional[str] = None):
        self.api_key = api_key
        self.proxy_url = proxy_url
        self.rate_limiter = RealDebridRateLimiter()
        self.request_handler = self._create_request_handler()
    
    def _create_request_handler(self):
        """Create a request handler with proper headers and rate limiting."""
        session = create_service_session()
        if self.proxy_url:
            session.proxies = {'http': self.proxy_url, 'https': self.proxy_url}
        session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/x-www-form-urlencoded'
        })
        
        handler = RealDebridRequestHandler(
            session=session,
            base_url='https://api.real-debrid.com/rest/1.0',
            request_logging=True
        )
        
        # Wrap execute method to enforce rate limiting
        original_execute = handler.execute
        def rate_limited_execute(*args, **kwargs):
            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                try:
                    with self.rate_limiter:
                        # Ensure endpoint starts with a single slash and has no trailing slash
                        if len(args) > 1 and isinstance(args[1], str):
                            endpoint = args[1].strip('/')
                            args = list(args)
                            args[1] = endpoint
                        return original_execute(*args, **kwargs)
                except Exception as e:
                    error_str = str(e)
                    if "404" in error_str:
                        # Don't retry 404s - resource doesn't exist
                        raise
                    elif "rate limit exceeded" in error_str.lower():
                        retry_count += 1
                        self.rate_limiter.handle_error()
                        if retry_count < max_retries:
                            backoff_time = min(2 ** retry_count, 30)  # Cap at 30 seconds
                            logger.debug(f"Rate limit hit, backing off for {backoff_time}s (attempt {retry_count}/{max_retries})")
                            time.sleep(backoff_time)
                            continue
                    raise
            raise RealDebridError("Max retries exceeded for rate limited request")
        
        handler.execute = rate_limited_execute
        return handler

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

    def check_link(self, link: str) -> dict:
        """Check if a link is valid and get its status."""
        return self.request_handler.execute(
            HttpMethod.POST,
            self.ENDPOINTS['unrestrict_check'],
            data={'link': link}
        )

    def unrestrict_link(self, link: str, password: Optional[str] = None) -> dict:
        """Unrestrict a link and get download information."""
        data = {'link': link}
        if password:
            data['password'] = password
        return self.request_handler.execute(
            HttpMethod.POST,
            self.ENDPOINTS['unrestrict_link'],
            data=data
        )

    def unrestrict_folder(self, link: str) -> dict:
        """Unrestrict a folder link and get its contents."""
        return self.request_handler.execute(
            HttpMethod.POST,
            self.ENDPOINTS['unrestrict_folder'],
            data={'link': link}
        )

    def decrypt_container_file(self, file_path: str) -> dict:
        """Decrypt a container file (dlc, ccf, rsdf)."""
        with open(file_path, 'rb') as f:
            files = {'file': f}
            return self.request_handler.execute(
                HttpMethod.PUT,
                self.ENDPOINTS['unrestrict_container_file'],
                files=files
            )

    def decrypt_container_link(self, link: str) -> dict:
        """Decrypt a container file from a link."""
        return self.request_handler.execute(
            HttpMethod.POST,
            self.ENDPOINTS['unrestrict_container_link'],
            data={'link': link}
        )

    def get_torrents_batch(self, torrent_ids: List[str]) -> List[Optional[Dict]]:
        """Get info for multiple torrents in an efficient manner.
        
        Args:
            torrent_ids: List of torrent IDs to fetch info for
            
        Returns:
            List of torrent info dictionaries, None for torrents that weren't found
        """
        results = []
        for i in range(0, len(torrent_ids), self.BATCH_SIZE):
            batch = torrent_ids[i:i + self.BATCH_SIZE]
            batch_results = []
            
            for torrent_id in batch:
                try:
                    info = self.request_handler.execute(
                        HttpMethod.GET,
                        f"{self.ENDPOINTS['torrent_info']}/{torrent_id}"
                    )
                    batch_results.append(info)
                except Exception as e:
                    if "404" in str(e):
                        batch_results.append(None)
                    else:
                        raise
                
            results.extend(batch_results)
            
            # Add delay between batches if not the last batch
            if i + self.BATCH_SIZE < len(torrent_ids):
                time.sleep(self.BATCH_DELAY)
                
        return results

    def delete_downloads_batch(self, download_ids: List[str]) -> List[bool]:
        """Delete multiple downloads in batches.
        
        Args:
            download_ids: List of download IDs to delete
            
        Returns:
            List of booleans indicating success/failure for each download
        """
        results = []
        for i in range(0, len(download_ids), self.BATCH_SIZE):
            batch = download_ids[i:i + self.BATCH_SIZE]
            batch_results = []
            
            for download_id in batch:
                try:
                    self.request_handler.execute(
                        HttpMethod.DELETE,
                        f"{self.ENDPOINTS['download_delete']}/{download_id}"
                    )
                    batch_results.append(True)
                except Exception as e:
                    if "404" in str(e):
                        batch_results.append(True)  # Count as success if already deleted
                    else:
                        batch_results.append(False)
                        logger.warning(f"Failed to delete download {download_id}: {e}")
                
            results.extend(batch_results)
            
            # Add delay between batches if not the last batch
            if i + self.BATCH_SIZE < len(download_ids):
                time.sleep(self.BATCH_DELAY)
                
        return results

    def delete_torrents_batch(self, torrent_ids: List[str]) -> List[bool]:
        """Delete multiple torrents in batches.
        
        Args:
            torrent_ids: List of torrent IDs to delete
            
        Returns:
            List of booleans indicating success/failure for each torrent
        """
        results = []
        for i in range(0, len(torrent_ids), self.BATCH_SIZE):
            batch = torrent_ids[i:i + self.BATCH_SIZE]
            batch_results = []
            
            for torrent_id in batch:
                try:
                    self.request_handler.execute(
                        HttpMethod.DELETE,
                        f"{self.ENDPOINTS['torrent_delete']}/{torrent_id}"
                    )
                    batch_results.append(True)
                except Exception as e:
                    if "404" in str(e):
                        batch_results.append(True)  # Count as success if already deleted
                    else:
                        batch_results.append(False)
                        logger.warning(f"Failed to delete torrent {torrent_id}: {e}")
                
            results.extend(batch_results)
            
            # Add delay between batches if not the last batch
            if i + self.BATCH_SIZE < len(torrent_ids):
                time.sleep(self.BATCH_DELAY)
                
        return results

    def unrestrict_links_batch(self, links: List[str]) -> List[Optional[Dict]]:
        """Unrestrict multiple links in batches.
        
        Args:
            links: List of links to unrestrict
            
        Returns:
            List of unrestricted link info dictionaries, None for failed links
        """
        results = []
        for i in range(0, len(links), self.BATCH_SIZE):
            batch = links[i:i + self.BATCH_SIZE]
            batch_results = []
            
            for link in batch:
                try:
                    info = self.request_handler.execute(
                        HttpMethod.POST,
                        self.ENDPOINTS['unrestrict_link'],
                        data={'link': link}
                    )
                    batch_results.append(info)
                except Exception as e:
                    logger.warning(f"Failed to unrestrict link {link}: {e}")
                    batch_results.append(None)
                
            results.extend(batch_results)
            
            # Add delay between batches if not the last batch
            if i + self.BATCH_SIZE < len(links):
                time.sleep(self.BATCH_DELAY)
                
        return results

    def check_downloads_availability_batch(self, download_ids: List[str]) -> List[Optional[Dict]]:
        """Check availability for multiple downloads in batches.
        
        Args:
            download_ids: List of download IDs to check
            
        Returns:
            List of availability info dictionaries, None for failed checks
        """
        results = []
        for i in range(0, len(download_ids), self.BATCH_SIZE):
            batch = download_ids[i:i + self.BATCH_SIZE]
            batch_results = []
            
            for download_id in batch:
                try:
                    info = self.request_handler.execute(
                        HttpMethod.GET,
                        f"{self.ENDPOINTS['downloads']}/{download_id}"
                    )
                    batch_results.append(info)
                except Exception as e:
                    if "404" in str(e):
                        batch_results.append(None)
                    else:
                        logger.warning(f"Failed to check download {download_id}: {e}")
                        batch_results.append(None)
                
            results.extend(batch_results)
            
            # Add delay between batches if not the last batch
            if i + self.BATCH_SIZE < len(download_ids):
                time.sleep(self.BATCH_DELAY)
                
        return results

class RealDebridRequestHandler(BaseRequestHandler):
    def handle_response(self, response: Response) -> Any:
        """Handle the API response with proper error handling."""
        try:
            if response.status_code == 404:
                raise RealDebridError(f"Resource not found: {response.url}")
            elif response.status_code == 403:
                raise RealDebridError("Account locked or permission denied")
            elif response.status_code == 401:
                raise RealDebridError("API token expired or invalid")
            elif response.status_code == 429:
                retry_after = response.headers.get('Retry-After', '60')
                raise RealDebridError(f"Rate limit exceeded. Retry after {retry_after}s")
            
            response.raise_for_status()
            return response.json() if response.content else None
            
        except json.JSONDecodeError:
            raise RealDebridError(f"Invalid JSON response: {response.text}")

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
    QUEUE_TIMEOUT = 60  # Increased from 30 to allow more time in queue
    MAX_ZERO_SEEDER_CHECKS = 3  # Increased from 2 to give more time for seeders
    MAX_PARALLEL_TORRENTS = 5
    INITIAL_CHECK_DELAY = 5
    TORRENT_ADD_INTERVAL = 5
    PARALLEL_CHECK_INTERVAL = 15
    RATE_LIMIT_BACKOFF_BASE = 2.0  # Base for exponential backoff
    RATE_LIMIT_INITIAL_DELAY = 5.0  # Initial delay in seconds
    RATE_LIMIT_MAX_DELAY = 60.0  # Maximum delay in seconds
    RATE_LIMIT_MAX_RETRIES = 5  # Maximum number of retries for rate limited requests

    # Constants for seeder checking
    SEEDER_CHECK_INTERVAL = 5  # Check every 5 seconds
    MAX_SEEDER_CHECKS = 4      # Check 4 times (0s, 5s, 10s, 15s = total 20s)
    INITIAL_CHECK_DELAY = 5    # Wait 5s before first check

    # Constants for cleanup thresholds
    CLEANUP_NO_SEEDERS_TIME = 120  # Time to wait before cleaning up torrents with 0 seeders (2 minutes)
    CLEANUP_SLOW_SPEED_TIME = 300  # Time to wait before cleaning up slow torrents (5 minutes)
    CLEANUP_NO_PROGRESS_TIME = 300  # Time to wait before cleaning up torrents with no progress (5 minutes)
    CLEANUP_MAGNET_TIME = 300  # Time to wait before cleaning up stuck magnet conversions (5 minutes)
    CLEANUP_FILE_SELECTION_TIME = 120  # Time to wait before cleaning up stuck file selections (2 minutes)
    CLEANUP_SPEED_THRESHOLD = 50000  # Minimum acceptable speed in bytes/s (50 KB/s)
    CLEANUP_PROGRESS_CHECK_INTERVAL = 60  # How often to check progress (1 minute)
    CLEANUP_BATCH_SIZE = 10  # Maximum number of torrents to delete in one batch

    def __init__(self, api: RealDebridAPI):
        """Initialize Real-Debrid downloader with thread-safe components"""
        self.api = api
        self.key = "real_debrid"  # Add key attribute for API identification
        self.rate_limiter = RealDebridRateLimiter()
        self.download_manager = DownloadManager()  # Initialize DownloadManager
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
            torrents = self.api.request_handler.execute(HttpMethod.GET, self.api.ENDPOINTS['torrents'])
            if not torrents:
                return 0

            # Get current downloads
            downloads = self.api.request_handler.execute(HttpMethod.GET, self.api.ENDPOINTS['downloads'])

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
        # Get all downloads in one request to minimize API calls
        try:
            downloads = self.api.request_handler.execute(HttpMethod.GET, self.api.ENDPOINTS['downloads'])
            downloads_by_torrent = {}
            for download in downloads:
                torrent_id = download.get("torrent_id")
                if torrent_id:
                    downloads_by_torrent[torrent_id] = []
                    for d in downloads:
                        if d.get("torrent_id") == torrent_id:
                            downloads_by_torrent[torrent_id].append(d['id'])
        except Exception as e:
            logger.warning(f"Failed to get downloads list: {e}")
            downloads_by_torrent = {}

        for torrent_id, reason in torrents:
            try:
                # Delete associated downloads if any
                if torrent_id in downloads_by_torrent:
                    for download_id in downloads_by_torrent[torrent_id]:
                        try:
                            self.api.request_handler.execute(HttpMethod.DELETE, f"{self.api.ENDPOINTS['download_delete']}/{download_id}")
                            logger.debug(f"Deleted download {download_id} associated with torrent {torrent_id}")
                            # Add a small delay between requests to respect rate limits
                            time.sleep(0.1)
                        except Exception as e:
                            logger.warning(f"Failed to delete download {download_id}: {e}")

                # Then delete the torrent
                self.api.request_handler.execute(HttpMethod.DELETE, f"{self.api.ENDPOINTS['torrent_delete']}/{torrent_id}")
                logger.info(f"Deleted torrent {torrent_id}: {reason}")
                deleted += 1
                # Add a small delay between torrent deletions
                time.sleep(0.1)
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
            downloads = self.api.request_handler.execute(HttpMethod.GET, self.api.ENDPOINTS['downloads'])
            if not isinstance(downloads, list):
                logger.error(f"Unexpected downloads response type: {type(downloads)}")
                return 0
                
            deleted = 0
            
            # Get current torrents for reference
            try:
                torrents = {t["id"]: t for t in self.api.request_handler.execute(HttpMethod.GET, self.api.ENDPOINTS['torrents'])}
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
                            self.api.request_handler.execute(HttpMethod.DELETE, f"{self.api.ENDPOINTS['download_delete']}/{download_id}")
                            deleted += 1
                            logger.info(f"🗑️ Deleted download {self._color_text(download_id, 'yellow')} ({filename}): {reason}, status: {status}")
                            
                            # Update our tracking
                            if content_id:
                                if download_id in self.active_downloads[content_id]:
                                    self.active_downloads[content_id].remove(download_id)
                        except Exception as e:
                            if "404" in str(e):
                                deleted += 1  # Already deleted
                                logger.debug(f"🗑️ Download {download_id} was already deleted")
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
            self.api.request_handler.execute(HttpMethod.POST, f"{self.api.ENDPOINTS['torrent_select']}/{torrent_id}", data=data)
            
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
        try:
            # First check active count
            active_info = self.api.request_handler.execute(
                HttpMethod.GET, 
                self.api.ENDPOINTS['torrents_active']
            )
            
            active_count = active_info.get('count', 0)
            if active_count > 0:
                logger.debug(f"🔍 Found {active_count} active torrents")
                
                # Then get full torrent list only if we have active torrents
                torrents = self.api.request_handler.execute(
                    HttpMethod.GET,
                    self.api.ENDPOINTS['torrents']
                )
                
                if not isinstance(torrents, list):
                    logger.error(f"Unexpected torrents response type: {type(torrents)}")
                    return DownloadCachedStreamResult(success=False, error="Invalid API response format")
                
                # Filter active torrents
                active_torrents = [
                    t['id'] for t in torrents 
                    if t.get('status') in ['downloading', 'magnet_conversion', 'waiting_files_selection']
                ]
                
                # Process torrents in smaller batches with rate limiting
                batch_size = 2  # Reduced batch size
                for i in range(0, len(active_torrents), batch_size):
                    batch = active_torrents[i:i + batch_size]
                    
                    # Process each batch
                    for torrent_id in batch:
                        info = self._get_torrent_info(torrent_id)
                        if info:
                            status = info.get('status')
                            if status in ['downloaded', 'uploading', 'magnet_conversion']:
                                self._process_torrent_status(torrent_id, info, item)
                        
                        # Add delay between requests in the same batch
                        time.sleep(1)  # Increased delay between requests
                    
                    # Add larger delay between batches
                    if i + batch_size < len(active_torrents):
                        time.sleep(2)  # Increased delay between batches
            else:
                logger.debug("No active torrents found")
            
            return self._finalize_download(item, stream)
            
        except Exception as e:
            logger.error(f"❌ Error in download_cached_stream: {str(e)}")
            if "rate limit exceeded" in str(e).lower():
                self.rate_limiter.handle_error()
            raise

    def _process_torrent_status(self, torrent_id: str, info: dict, item: MediaItem):
        """Process torrent status with rate limiting awareness"""
        try:
            status = info.get('status', 'unknown')
            filename = info.get('filename', 'unknown')
            progress = info.get('progress', 0)
            
            if status == 'downloaded':
                logger.info(f"✅ Torrent {torrent_id} downloaded: {filename}")
                self._handle_downloaded_torrent(torrent_id, info, item)
            elif status == 'uploading':
                logger.debug(f"⬆️ Torrent {torrent_id} uploading: {progress}%")
            elif status == 'magnet_conversion':
                logger.debug(f"🧲 Torrent {torrent_id} converting magnet")
            else:
                logger.debug(f"ℹ️ Torrent {torrent_id} status: {status}")
                
        except Exception as e:
            logger.error(f"❌ Error processing torrent {torrent_id}: {str(e)}")

    def _finalize_download(self, item: MediaItem, stream: Stream) -> DownloadCachedStreamResult:
        """Finalize download by checking for completed torrents"""
        try:
            # Get completed torrents
            completed_torrents = self.api.request_handler.execute(HttpMethod.GET, self.api.ENDPOINTS['downloads'])

            # Find matching torrent
            for torrent in completed_torrents:
                torrent_id = torrent.get("id", "")
                info = self._get_torrent_info(torrent_id)
                if info:
                    # Check if this torrent matches our item
                    if info.get("filename") == item.name:
                        logger.info(f"✅ Found matching torrent {torrent_id} for {item.name}")
                        return DownloadCachedStreamResult(success=True, torrent_id=torrent_id, info=info)
            
            # If no matching torrent found, return error
            return DownloadCachedStreamResult(success=False, error="No matching torrent found")
        
        except Exception as e:
            logger.error(f"❌ Error finalizing download: {str(e)}")
            return DownloadCachedStreamResult(success=False, error=str(e))

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
        seeder_check_count = 0
        queue_start_time = None
        last_progress = 0
        stall_start_time = None
        
        # Wait initial delay before first check
        time.sleep(self.INITIAL_CHECK_DELAY)
        
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

                status = info.get("status", "").lower()
                filename = info.get("filename", "unknown")
                progress = info.get("progress", 0)
                speed = info.get("speed", 0) / 1024 / 1024  # Convert to MB/s
                seeders = info.get("seeders", 0)

                # Log status with content name
                logger.debug(f"📊 {self._format_progress(progress, speed, seeders, item)}\n"
                           f"🎬 File: {filename}\n"
                           f"⏱️ Elapsed: {elapsed:.1f}s")

                # Check seeders
                should_continue, error = self._check_seeders(info, elapsed, seeder_check_count)
                if not should_continue:
                    return DownloadCachedStreamResult(success=False, error=error)
                    
                seeder_check_count += 1
                
                # Manage multiple torrents of same content
                should_continue, error = self._manage_content_torrents(content_id, torrent_id)
                if not should_continue:
                    return DownloadCachedStreamResult(success=False, error=error)

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
                        return DownloadCachedStreamResult(
                            success=False, 
                            error=f"Queue timeout after {queue_time:.1f}s"
                        )
                    
                    last_check_time = current_time
                    continue

                elif status == RDTorrentStatus.DOWNLOADING.value:
                    if progress is not None:
                        # Check for stalled download
                        if progress == last_progress and speed == 0:
                            if stall_start_time is None:
                                stall_start_time = current_time
                            elif current_time - stall_start_time > self.BASE_TIMEOUT:
                                return DownloadCachedStreamResult(
                                    success=False, 
                                    error=f"Download stalled at {progress:.1f}% for {(current_time - stall_start_time):.1f}s"
                                )
                        else:
                            stall_start_time = None
                            last_progress = progress
                        
                        # Calculate timeout based on progress and file size
                        timeout = self._calculate_timeout(info, item)
                        if elapsed > timeout:
                            return DownloadCachedStreamResult(
                                success=False, 
                                error=f"Download timeout after {elapsed:.1f}s"
                            )
                    
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
                    error_msg = info.get("error", "Unknown error")
                    return DownloadCachedStreamResult(success=False, error=f"Torrent error: {error_msg}")

            except Exception as e:
                logger.error(f"Error checking download status: {e}")
                return DownloadCachedStreamResult(success=False, error=str(e))

            last_check_time = current_time

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
            # Get user account info
            response = self.api.request_handler.execute(HttpMethod.GET, '/user')
            
            if not isinstance(response, dict):
                logger.error("Invalid response format from Real-Debrid API")
                return False
                
            # Check premium status
            premium = response.get('premium', False)
            if not premium:
                logger.error("Account is not premium")
                return False
                
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
        """Add a torrent to Real-brid and select files"""
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
            if not result or not result.torrent_id:
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
        base_backoff = 5  # Increased base delay to 5 seconds
        
        try:
            # Check active torrents before adding
            active_count = self.api.request_handler.execute(HttpMethod.GET, self.api.ENDPOINTS['torrents_active'])
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
                try:
                    response = self.api.request_handler.execute(
                        HttpMethod.POST,
                        self.api.ENDPOINTS['magnet_add'],
                        data={"magnet": magnet}
                    )
                    
                    torrent_id = response.get("id")
                    if torrent_id:
                        logger.debug(f"✅ Successfully added torrent {torrent_id}")
                        # Mark success to reduce backoff
                        self.rate_limiter.mark_success()
                        return TorrentAddResult(success=True, error=None, torrent_id=torrent_id, info=response)
                    else:
                        logger.error("❌ Empty response from Real-Debrid API")
                        return TorrentAddResult(success=False, error="Empty response from Real-Debrid API", torrent_id="", info={})
            
                except requests.HTTPError as http_err:
                    if http_err.response.status_code == 503:
                        # Don't count 503 errors as rate limit failures
                        logger.warning(f"⚠️ Real-Debrid API temporarily unavailable (503)")
                        backoff = min(base_backoff * (2 ** (attempt - 1)), 60)  # Cap at 60 seconds for 503s
                        if attempt < max_attempts:
                            logger.warning(f"⏳ Service unavailable, retrying in {backoff}s... (attempt {attempt}/{max_attempts})")
                            time.sleep(backoff)
                            return self._add_magnet_or_torrent(magnet=magnet, torrent=torrent, attempt=attempt + 1)
                        raise
                    elif http_err.response.status_code == 429:  # Rate limit exceeded
                        # Let the rate limiter handle the backoff
                        self.rate_limiter.mark_failure()
                        raise
                    else:
                        raise
                
        except Exception as e:
            if "Active Limit Exceeded" in str(e):
                # Try cleanup and retry quickly
                logger.warning(f"⚠️ Active limit exceeded, retrying cleanup...")
                cleaned = self._cleanup_inactive_torrents()
                backoff = 1 if cleaned > 0 else base_backoff  # Retry quickly if we cleaned something
            else:
                backoff = min(base_backoff * (2 ** (attempt - 1)), 60)  # Cap at 60 seconds
        
            if attempt < max_attempts:
                logger.warning(f"⏳ Retrying in {backoff}s... (attempt {attempt}/{max_attempts})")
                time.sleep(backoff)
                return self._add_magnet_or_torrent(magnet=magnet, torrent=torrent, attempt=attempt + 1)
            else:
                logger.error(f"❌ Failed to add torrent after {max_attempts} attempts: {e}")
                return TorrentAddResult(success=False, error=f"Failed to add torrent after {max_attempts} attempts: {e}", torrent_id="", info={})
    
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

    def _format_progress(self, progress: float, speed: float, seeders: int, item: Optional[MediaItem] = None) -> str:
        """Format progress info with colors"""
        # Format progress percentage
        progress_str = f"{progress:.1f}%" if progress is not None else "?.?%"
        if progress == 0:
            progress_str = self._color_text(progress_str, "red")
        elif progress < 50:
            progress_str = self._color_text(progress_str, "yellow")
        else:
            progress_str = self._color_text(progress_str, "green")
            
        # Format speed
        speed_str = f"{speed:.1f}MB/s" if speed > 1 else f"{speed*1024:.1f}KB/s" if speed > 0 else "0KB/s"
        if speed == 0:
            speed_str = self._color_text(speed_str, "red")
        elif speed < 1:  # Less than 1 MB/s
            speed_str = self._color_text(speed_str, "yellow")
        else:
            speed_str = self._color_text(speed_str, "green")
            
        # Format seeders
        seeders_str = str(seeders)
        if seeders == 0:
            seeders_str = self._color_text(seeders_str, "red")
        elif seeders < 5:
            seeders_str = self._color_text(seeders_str, "yellow")
        else:
            seeders_str = self._color_text(seeders_str, "green")

        # Format content name if available
        content_str = ""
        if item:
            if hasattr(item, 'name') and callable(item.name):
                name = item.name()
            else:
                name = getattr(item, 'title', 'Unknown')
            content_str = f" [{self._color_text(name, 'cyan')}]"
            
        return f"{progress_str} 🌱{seeders_str} ⚡{speed_str}{content_str}"

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

    def _cleanup_all_except(self, exclude_ids: Set[str] = None) -> int:
        """Clean up all non-downloaded torrents except those in exclude_ids"""
        if exclude_ids is None:
            exclude_ids = set()
            
        logger.debug(f"🧹 Cleaning up all torrents except {len(exclude_ids)} excluded IDs")
        
        try:
            # Get list of all torrents with rate limit handling
            with self.rate_limiter:
                torrents = self.api.request_handler.execute(HttpMethod.GET, self.api.ENDPOINTS['torrents'])
                
            if not torrents:
                logger.warning("No torrents found to clean up")
                return 0

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
            to_delete = []  # List of (priority, torrent_id, reason) tuples
            
            # Track torrents by various attributes
            filename_to_torrents = defaultdict(list)  # Track potential duplicates
            status_counts = defaultdict(int)  # Count torrents by status
            total_active = 0
            
            for torrent in torrents:
                status = torrent.get("status", "")
                if not self._is_active_status(status):
                    continue
                    
                total_active += 1
                status_counts[status] += 1
                
                # Calculate elapsed time
                time_elapsed = 0
                try:
                    added = torrent.get("added", "")
                    if added:
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
                    "id": torrent.get("id", ""),
                    "size": torrent.get("bytes", 0)
                }
                
                # Track potential duplicates
                filename_to_torrents[torrent_stats["filename"]].append(torrent_stats)
                
                # Priority-based cleanup checks
                cleanup_priority = None
                cleanup_reason = None
                
                if status == "downloading":
                    # 1. Highest Priority: No seeders and no progress
                    if torrent_stats["speed"] == 0 and torrent_stats["seeders"] == 0:
                        if time_elapsed > self.CLEANUP_NO_SEEDERS_TIME:
                            cleanup_priority = 90
                            cleanup_reason = (f"stalled with no seeders for {time_elapsed:.0f}s "
                                           f"(progress: {torrent_stats['progress']:.1f}%)")
                    
                    # 2. High Priority: No progress at all
                    elif torrent_stats["progress"] == 0:
                        if time_elapsed > self.CLEANUP_NO_PROGRESS_TIME:
                            cleanup_priority = 85
                            cleanup_reason = f"no progress in {time_elapsed:.0f}s"
                    
                    # 3. Medium Priority: Extremely slow download
                    elif torrent_stats["speed"] < self.CLEANUP_SPEED_THRESHOLD:
                        if time_elapsed > self.CLEANUP_SLOW_SPEED_TIME:
                            speed_kb = torrent_stats["speed"] / 1024
                            cleanup_priority = 80
                            cleanup_reason = (f"slow speed ({speed_kb:.1f} KB/s) for {time_elapsed:.0f}s "
                                           f"(progress: {torrent_stats['progress']:.1f}%)")
                
                # 4. Medium Priority: Stuck in magnet conversion
                elif status == "magnet_conversion":
                    if time_elapsed > self.CLEANUP_MAGNET_TIME:
                        cleanup_priority = 70
                        cleanup_reason = f"stuck in magnet conversion for {time_elapsed:.0f}s"
                
                # 5. Medium Priority: Stuck waiting for file selection
                elif status == "waiting_files_selection":
                    if time_elapsed > self.CLEANUP_FILE_SELECTION_TIME:
                        cleanup_priority = 75
                        cleanup_reason = f"stuck waiting for file selection for {time_elapsed:.0f}s"
                
                if cleanup_priority and cleanup_reason:
                    # Add file details to reason
                    size_str = self._format_size(torrent_stats["size"])
                    full_reason = f"{cleanup_reason} (file: {torrent_stats['filename']}, size: {size_str})"
                    to_delete.append((cleanup_priority, torrent_stats["id"], full_reason))
            
            # Log status summary
            if status_counts:
                status_summary = [f"{status}: {count}" for status, count in status_counts.items()]
                logger.debug(f"Active torrent status: {', '.join(status_summary)}")
            
            # Handle duplicates only if we're being aggressive
            if aggressive_cleanup:
                for filename, dupes in filename_to_torrents.items():
                    if len(dupes) > 1:
                        # Sort by progress (highest first), then speed, then seeders
                        dupes.sort(key=lambda x: (x["progress"], x["speed"], x["seeders"]), reverse=True)
                        best = dupes[0]
                        
                        # Delete all duplicates except the best one
                        for dupe in dupes[1:]:
                            reason = (f"duplicate of {best['filename']} "
                                    f"(this: {dupe['progress']:.1f}% @ {dupe['speed']/1024:.1f}KB/s, "
                                    f"best: {best['progress']:.1f}% @ {best['speed']/1024:.1f}KB/s)")
                            to_delete.append((60, dupe["id"], reason))
            
            # Sort by priority (highest first) and limit batch size
            if to_delete:
                to_delete.sort(reverse=True)
                to_delete = to_delete[:self.CLEANUP_BATCH_SIZE]
                
                # Log what we're about to delete
                logger.info(f"Cleaning up {len(to_delete)} torrents:")
                for _, torrent_id, reason in to_delete:
                    logger.info(f"  • {self._format_deletion(reason)}")
                
                # Convert to format needed by batch delete
                delete_batch = [(t[1], t[2]) for t in to_delete]
                cleaned = self._batch_delete_torrents(delete_batch)
                
                if cleaned:
                    logger.info(f"✅ Successfully cleaned up {cleaned} torrents")
                else:
                    logger.warning("⚠️ Failed to clean up any torrents")
                
                self.last_cleanup_time = current_time
                return cleaned
            
            return 0
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return 0

    def _cleanup_inactive_torrents(self, aggressive: bool = False) -> int:
        """Clean up inactive torrents and their associated downloads.
        
        Args:
            aggressive: If True, use more aggressive cleanup criteria
            
        Returns:
            Number of torrents deleted
        """
        try:
            # Get list of all torrents
            torrents = self.api.request_handler.execute(HttpMethod.GET, self.api.ENDPOINTS['torrents'])
            if not torrents:
                logger.debug("No torrents found to clean up")
                return 0

            # Get all downloads in one request
            downloads = self.api.request_handler.execute(HttpMethod.GET, self.api.ENDPOINTS['downloads'])
            downloads_by_torrent = {}
            for download in downloads:
                torrent_id = download.get("torrent_id")
                if torrent_id:
                    downloads_by_torrent.setdefault(torrent_id, []).append(download)

            # Collect torrent IDs to process
            torrent_ids = [t['id'] for t in torrents if 'id' in t]
            if not torrent_ids:
                return 0

            # Get detailed info for all torrents in batches
            torrent_infos = self.api.get_torrents_batch(torrent_ids)
            
            # Process results and collect items to delete
            downloads_to_delete = []
            torrents_to_delete = []
            
            for torrent_id, info in zip(torrent_ids, torrent_infos):
                if info is None:  # Torrent not found
                    continue
                    
                reason = self._should_delete_torrent(info, aggressive)
                if reason:
                    # Collect associated downloads
                    for download in downloads_by_torrent.get(torrent_id, []):
                        download_id = download.get('id')
                        if download_id:
                            downloads_to_delete.append(download_id)
                    
                    torrents_to_delete.append(torrent_id)
                    logger.info(f"Marking torrent {torrent_id} for deletion: {reason}")

            # Delete downloads in batches
            if downloads_to_delete:
                logger.debug(f"Deleting {len(downloads_to_delete)} downloads in batches")
                self.api.delete_downloads_batch(downloads_to_delete)

            # Delete torrents in batches
            if torrents_to_delete:
                logger.debug(f"Deleting {len(torrents_to_delete)} torrents in batches")
                self.api.delete_torrents_batch(torrents_to_delete)

            return len(torrents_to_delete)

        except Exception as e:
            logger.error(f"Error cleaning up inactive torrents: {e}")
            return 0

    def _get_torrent_info(self, torrent_id: str) -> Optional[Dict]:
        """Get detailed info for a torrent with error handling and caching."""
        try:
            # Use class-level cache to avoid duplicate requests
            if not hasattr(self, '_torrent_info_cache'):
                self._torrent_info_cache = {}
            
            # Check cache first
            cache_key = f"{torrent_id}_{int(time.time() / 30)}"  # Cache for 30 seconds
            if cache_key in self._torrent_info_cache:
                return self._torrent_info_cache[cache_key]
            
            logger.debug(f"📡 Fetching info for torrent {torrent_id}")
            try:
                info = self.api.request_handler.execute(
                    HttpMethod.GET,
                    f"{self.api.ENDPOINTS['torrent_info']}/{torrent_id}"
                )
                # Cache the result only if successful
                self._torrent_info_cache[cache_key] = info
                return info
            except Exception as e:
                error_str = str(e)
                if "404" in error_str:
                    # Torrent doesn't exist anymore, remove from cache
                    logger.debug(f"Torrent {torrent_id} not found (404), likely already deleted")
                    if torrent_id in self._torrent_info_cache:
                        del self._torrent_info_cache[torrent_id]
                    return None
                elif "401" in str(e):
                    logger.error("API token expired or invalid")
                    raise
                elif "403" in str(e):
                    logger.error("Account locked or permission denied")
                    raise
                else:
                    logger.error(f"❌ Error getting torrent info for {torrent_id}: {error_str}")
                    return None
            
        except Exception as e:
            logger.error(f"❌ Error getting torrent info for {torrent_id}: {str(e)}")
            return None

    def get_torrent_info(self, torrent_id: str) -> dict:
        """
        Get information about a torrent
        Required by DownloaderBase
        """
        if not self.initialized:
            raise RealDebridError("Downloader not properly initialized")

        response = self.api.request_handler.execute(
            HttpMethod.GET,
            f"{self.api.ENDPOINTS['torrent_info']}/{torrent_id}"
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
                f"🎬 Processing file: {filename}"
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
                f"{self.api.ENDPOINTS['torrent_delete']}/{torrent_id}"
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

    def _check_seeders(self, info: dict, elapsed: float, seeder_check_count: int) -> Tuple[bool, str]:
        """
        Check if torrent has seeders within the configured check pattern.
        
        Args:
            info: Torrent info dictionary
            elapsed: Time elapsed since start
            seeder_check_count: Number of seeder checks performed
            
        Returns:
            Tuple of (should_continue, error_message)
            - should_continue: True if download should continue, False if it should stop
            - error_message: Error message if should_continue is False, empty string otherwise
        """
        seeders = info.get("seeders", 0)
        
        # Only check seeders during initial period
        if elapsed <= (self.SEEDER_CHECK_INTERVAL * self.MAX_SEEDER_CHECKS):
            if seeders == 0:
                if seeder_check_count >= self.MAX_SEEDER_CHECKS:
                    error = f"No seeders found after {seeder_check_count} checks ({elapsed:.1f}s)"
                    logger.warning(error)
                    return False, error
                return True, ""
            else:
                # Found seeders, log and continue
                logger.info(f"Found {seeders} seeders after {seeder_check_count} checks")
                return True, ""
                
        return True, ""

    def _manage_content_torrents(self, content_id: str, current_torrent_id: str) -> Tuple[bool, str]:
        """
        Manage multiple torrents of the same content.
        After 4 torrents of same content, keep only the one with highest seeders.
        
        Args:
            content_id: ID of the content being downloaded
            current_torrent_id: ID of the current torrent being checked
            
        Returns:
            Tuple of (should_continue, error_message)
            - should_continue: True if current torrent should continue, False if it should stop
            - error_message: Error message if should_continue is False, empty string otherwise
        """
        active_torrents = self.active_downloads[content_id]
        if len(active_torrents) < 4:
            return True, ""
            
        # Get seeder info for all active torrents
        torrent_seeders = {}
        for torrent_id in active_torrents:
            info = self.get_torrent_info(torrent_id)
            if info:
                torrent_seeders[torrent_id] = info.get("seeders", 0)
            else:
                torrent_seeders[torrent_id] = 0
                
        # If all have 0 seeders, remove all and move to next content
        if all(seeders == 0 for seeders in torrent_seeders.values()):
            for torrent_id in active_torrents.copy():
                self.delete_torrent(torrent_id)
                self._remove_active_download(content_id, torrent_id)
            return False, "All torrents have 0 seeders, moving to next content"
            
        # Keep only the torrent with highest seeders
        best_torrent = max(torrent_seeders.items(), key=lambda x: x[1])[0]
        
        # If current torrent is not the best one, remove it
        if current_torrent_id != best_torrent:
            self.delete_torrent(current_torrent_id)
            self._remove_active_download(content_id, current_torrent_id)
            return False, f"Removed in favor of torrent with higher seeders ({torrent_seeders[best_torrent]} seeders)"
            
        # Remove all other torrents
        for torrent_id in active_torrents.copy():
            if torrent_id != best_torrent:
                self.delete_torrent(torrent_id)
                self._remove_active_download(content_id, torrent_id)
                
        return True, ""

    def _get_media_file_ids(self, info: dict) -> List[str]:
        """
        !!! CRITICAL METHOD - DO NOT REMOVE !!!
        This method is essential for the torrent file selection process.
        It is used by add_torrent() to identify which files to download.
        
        Get the file IDs of media files from torrent info.
        
        Args:
            info (dict): Torrent info dictionary from Real-Debrid API containing:
                - files (List[dict]): List of file info dictionaries with:
                    - path (str): File path/name
                    - id (str/int): File ID
                
        Returns:
            List[str]: List of file IDs for media files that match VIDEO_EXTENSIONS
            
        Example:
            >>> info = {"files": [{"path": "movie.mkv", "id": "1"}, {"path": "sample.txt", "id": "2"}]}
            >>> self._get_media_file_ids(info)
            ["1"]
        """
        files = info.get("files", [])
        if not files:
            return []
            
        media_files = []
        for file in files:
            filename = file.get("path", "").lower()
            if any(filename.endswith(ext) for ext in VIDEO_EXTENSIONS):
                file_id = str(file.get("id", ""))
                if file_id:
                    media_files.append(file_id)
                    
        return media_files

    def _get_available_files(self, downloads: List[Dict]) -> List[Dict]:
        """Get available files from a list of downloads in batches."""
        if not downloads:
            return []

        # Extract download IDs
        download_ids = [d['id'] for d in downloads if 'id' in d]
        if not download_ids:
            return []

        # Check availability in batches
        availability_infos = self.api.check_downloads_availability_batch(download_ids)
        
        available_files = []
        for download, availability in zip(downloads, availability_infos):
            if not availability:
                continue
                
            if availability.get('status') == 'downloaded':
                available_files.append(download)
                
        return available_files

    def _process_links_batch(self, links: List[str]) -> List[Dict]:
        """Process multiple links in batches."""
        if not links:
            return []

        # Unrestrict links in batches
        unrestricted_infos = self.api.unrestrict_links_batch(links)
        
        processed_links = []
        for link, info in zip(links, unrestricted_infos):
            if not info:
                logger.warning(f"Failed to process link: {link}")
                continue
                
            processed_links.append(info)
                
        return processed_links

    async def get_streams(self, magnet_link: str) -> List[Dict]:
        """Get available streams for a magnet link."""
        try:
            # Add the torrent
            result = await self._add_torrent(magnet_link)
            if not result.success:
                return []

            torrent_id = result.torrent_id
            info = result.info

            # Get available files
            files = info.get('files', [])
            if not files:
                return []

            # Get links for all files
            links = []
            for file in files:
                try:
                    link = await self._get_download_link(torrent_id, file['id'])
                    if link:
                        links.append(link)
                except Exception as e:
                    logger.warning(f"Failed to get download link for file {file['id']}: {e}")

            # Process all links in batches
            processed_links = self._process_links_batch(links)
            
            return processed_links

        except Exception as e:
            logger.error(f"Error getting streams: {e}")
            return []

    async def cleanup(self, aggressive: bool = False) -> None:
        """Clean up downloads and torrents."""
        try:
            # Get all downloads first
            downloads = self.api.request_handler.execute(HttpMethod.GET, self.api.ENDPOINTS['downloads'])
            if downloads:
                # Process downloads in batches for deletion
                download_ids = [d['id'] for d in downloads if self._should_delete_download(d)]
                if download_ids:
                    logger.info(f"Cleaning up {len(download_ids)} downloads")
                    self.api.delete_downloads_batch(download_ids)

            # Clean up torrents
            deleted = self._cleanup_inactive_torrents(aggressive)
            if deleted > 0:
                logger.info(f"Cleaned up {deleted} torrents")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")