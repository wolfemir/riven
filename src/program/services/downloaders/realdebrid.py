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
    VIDEO_EXTENSIONS,
    DownloadCachedStreamResult,
    DownloaderBase,
    FileFinder,
    premium_days_left,
)

class RDTorrentStatus(str, Enum):
    """Real-Debrid torrent status enumeration"""
    MAGNET_ERROR = "magnet_error"
    MAGNET_CONVERSION = "magnet_conversion"
    WAITING_FILES_SELECTION = "waiting_files_selection"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    ERROR = "error"
    SEEDING = "seeding"
    DEAD = "dead"
    UPLOADING = "uploading"
    COMPRESSING = "compressing"
    QUEUED = "queued"
    UNKNOWN = "unknown"

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
    """Base exception for Real-Debrid related errors"""

class TorrentNotFoundError(RealDebridError):
    """Raised when a torrent is not found on Real-Debrid servers"""

class InvalidFileIDError(RealDebridError):
    """Raised when invalid file IDs are provided"""

class DownloadFailedError(RealDebridError):
    """Raised when a torrent download fails"""

class QueuedTooManyTimesError(RealDebridError):
    """Raised when a torrent is queued too many times"""

class RealDebridActiveLimitError(RealDebridError):
    """Raised when Real-Debrid's active torrent limit is exceeded"""

class RealDebridRequestHandler(BaseRequestHandler):
    def __init__(self, session: Session, base_url: str, request_logging: bool = False):
        super().__init__(session, response_type=ResponseType.DICT, base_url=base_url, custom_exception=RealDebridError, request_logging=request_logging)

    def execute(self, method: HttpMethod, endpoint: str, **kwargs) -> Union[dict, list]:
        response = super()._request(method, endpoint, **kwargs)
        # Handle 202 (action already done) as success
        if response.status_code in (204, 202):
            return {}
        if not response.data and not response.is_ok:
            raise RealDebridError("Invalid JSON response from RealDebrid")
        return response.data

class RealDebridAPI:
    """Handles Real-Debrid API communication"""
    BASE_URL = "https://api.real-debrid.com/rest/1.0"

    def __init__(self, api_key: str, proxy_url: Optional[str] = None):
        self.api_key = api_key
        rate_limit_params = get_rate_limit_params(per_minute=60)
        self.session = create_service_session(rate_limit_params=rate_limit_params)
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})
        if proxy_url:
            self.session.proxies = {"http": proxy_url, "https": proxy_url}
        self.request_handler = RealDebridRequestHandler(self.session, self.BASE_URL)

class RealDebridRateLimiter:
    """Manages API rate limiting across threads"""
    def __init__(self):
        self.last_request_time = 0
        self.min_interval = 1.0  # Minimum time between requests in seconds
        self._lock = threading.Lock()

    def wait_if_needed(self):
        """Wait if necessary to respect rate limits"""
        with self._lock:
            current_time = time.time()
            elapsed = current_time - self.last_request_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_request_time = time.time()

class DownloadManager:
    """Manages concurrent downloads and threads"""
    def __init__(self, max_concurrent: int = 5):
        self.max_concurrent = max_concurrent
        self.active_downloads = 0
        self._lock = threading.Lock()
        self._download_complete = threading.Event()
        
    def wait_for_slot(self):
        """Wait until a download slot is available"""
        while True:
            with self._lock:
                if self.active_downloads < self.max_concurrent:
                    self.active_downloads += 1
                    return
            time.sleep(1)
            
    def release_slot(self):
        """Release a download slot"""
        with self._lock:
            self.active_downloads = max(0, self.active_downloads - 1)
            if self.active_downloads == 0:
                self._download_complete.set()

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
    STATUS_CHECK_INTERVAL = 5  # seconds
    QUEUE_TIMEOUT = 30  # seconds
    MAX_ZERO_SEEDER_CHECKS = 2  # Maximum number of zero seeder checks

    def __init__(self, api: RealDebridAPI):
        """Initialize Real-Debrid downloader with thread-safe components"""
        self.api = api
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
                                    logger.debug(f"Skipping deletion of {download_id} ({filename}): status changed to downloaded")
                                    if content_id:
                                        self.download_complete[content_id] = True
                                    continue
                                elif current_status == "unknown":
                                    logger.debug(f"Skipping deletion of {download_id} ({filename}): status is unknown")
                                    continue
                        except Exception as e:
                            logger.debug(f"Failed to double-check download status for {download_id}: {e}")
                        
                        try:
                            self.api.request_handler.execute(HttpMethod.DELETE, f"downloads/delete/{download_id}")
                            deleted += 1
                            logger.info(f"Deleted download {download_id} ({filename}): {reason}, status: {status}")
                            
                            # Update our tracking
                            if content_id:
                                if download_id in self.active_downloads[content_id]:
                                    self.active_downloads[content_id].remove(download_id)
                        except Exception as e:
                            if "404" in str(e):
                                deleted += 1  # Already deleted
                                logger.debug(f"Download {download_id} was already deleted")
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
        logger.debug(f"Processing {len(files)} files from Real-Debrid")
        result = {}
        
        # If no files yet, return empty result to trigger retry
        if not files:
            logger.debug("No files available yet, will retry")
            return {}
        
        # Process all video files
        valid_videos = []
        
        for file in files:
            path = file.get("path", "")
            name = path.lower()
            size = file.get("bytes", 0)
            file_id = str(file.get("id", ""))
            
            # Skip if no valid ID
            if not file_id:
                logger.debug(f"✗ Skipped file with no ID: {path}")
                continue
        
            # Skip sample files and unwanted files
            if "/sample/" in name.lower() or "sample" in name.lower():
                logger.debug(f"✗ Skipped sample file: {name}")
                continue
            
            if any(name.endswith(f".{ext}") for ext in VIDEO_EXTENSIONS):
                # For shows, check if this file matches the requested episode
                if item and hasattr(item, 'type') and item.type == "episode":
                    # Parse episode info from filename
                    try:
                        parsed = parse(name)
                        if parsed and parsed.season_number is not None and parsed.episode_numbers:
                            # Check if season/episode numbers match
                            if (parsed.season_number == item.parent.number and 
                                item.number in parsed.episode_numbers):
                                valid_videos.append(file)
                                logger.debug(f"✓ Found matching episode: {name} (S{parsed.season_number:02d}E{item.number:02d})")
                            else:
                                logger.debug(f"✗ Episode numbers don't match: {name} vs S{item.parent.number:02d}E{item.number:02d}")
                        else:
                            logger.debug(f"✗ Could not parse episode info from: {name}")
                    except Exception as e:
                        logger.debug(f"✗ Error parsing episode info from {name}: {e}")
                else:
                    # For movies or when no item is provided, include all video files
                    valid_videos.append(file)
                    logger.debug(f"✓ Found valid video file: {name} (size: {size} bytes, id: {file_id})")
            else:
                # Log why file was rejected
                logger.debug(f"✗ Skipped non-video file: {name}")

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
            logger.debug(f"✓ Selected file for download: {path} (size: {size} bytes, id: {file_id})")

        if not result:
            # Log all files for debugging
            logger.debug("No valid video files found. Available files:")
            for file in files:
                logger.debug(f"- {file.get('path', '')} ({file.get('bytes', 0)} bytes)")
        else:
            logger.debug(f"Selected {len(result)} video files for download")

        return result

    def select_files(self, torrent_id: str, file_ids: List[str]) -> bool:
        """Select files for download"""
        try:
            if not file_ids:
                logger.error("No files to select")
                return False

            data = {"files": ",".join(file_ids)}
            self.api.request_handler.execute(HttpMethod.POST, f"torrents/selectFiles/{torrent_id}", data=data)
            return True
        except Exception as e:
            logger.error(f"Failed to select files: {e}")
            return False

    def download_cached_stream(self, item: MediaItem, stream: Stream) -> DownloadCachedStreamResult:
        """Download a stream from Real-Debrid"""
        try:
            # Do a single thorough cleanup before starting new download
            exclude_ids = {stream.id} if hasattr(stream, 'id') else set()
            if not self._cleanup_all_except(exclude_ids):
                logger.warning("Initial cleanup failed, but continuing with download")

            # Check if we can start a new download
            if not self._can_start_download(item.id):
                return DownloadCachedStreamResult(
                    success=False,
                    error="Too many concurrent downloads"
                )

            # Add the torrent to Real-Debrid
            torrent_info = self._add_magnet_or_torrent(stream)
            if not torrent_info:
                return DownloadCachedStreamResult(
                    success=False,
                    error="Failed to add torrent"
                )

            torrent_id = torrent_info['id']
            self._add_active_download(item.id, torrent_id)

            try:
                # Process available files and select the ones to download
                files = torrent_info.get('files', [])
                valid_files = self._process_files(files, item)
                if not valid_files:
                    logger.error("No valid video files found in torrent")
                    return DownloadCachedStreamResult(
                        success=False,
                        error="No valid video files found"
                    )

                # Select the files to download
                file_ids = list(valid_files.keys())
                if not self.select_files(torrent_id, file_ids):
                    return DownloadCachedStreamResult(
                        success=False,
                        error="Failed to select files"
                    )

                # Wait for download to complete
                download_result = self.wait_for_download(torrent_id, item.id, item, stream)
                return download_result

            finally:
                # Always remove from active downloads
                self._remove_active_download(item.id, torrent_id)

        except Exception as e:
            logger.error(f"Error downloading stream: {e}")
            return DownloadCachedStreamResult(
                success=False,
                error=str(e)
            )

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

                status = info.get("status", "").lower()  # Convert to lowercase for comparison
                filename = info.get("filename", "unknown")
                progress = info.get("progress", 0)
                speed = info.get("speed", 0) / 1024 / 1024  # Convert to MB/s
                seeders = info.get("seeders", 0)

                logger.debug(f"Processing status: {status} for file {filename}")

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
                        logger.debug(f"Selected files {file_ids} for torrent {torrent_id}")
                    except Exception as e:
                        return DownloadCachedStreamResult(success=False, error=f"Failed to select files: {e}")
                    
                    last_check_time = current_time
                    continue

                elif status == RDTorrentStatus.QUEUED.value:
                    if queue_start_time is None:
                        queue_start_time = current_time
                        logger.debug(f"{filename} queued for download")
                    
                    queue_time = current_time - queue_start_time
                    if queue_time > self.QUEUE_TIMEOUT:
                        return DownloadCachedStreamResult(success=False, error=f"Queue timeout after {queue_time:.1f}s")
                    
                    last_check_time = current_time
                    continue

                elif status == RDTorrentStatus.DOWNLOADING.value:
                    if progress is not None:
                        logger.debug(f"{filename} - Progress: {progress}%, Speed: {speed:.2f}MB/s, Seeders: {seeders}")
                        
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
                        error=f"Download failed with status: {status}"
                    )

                else:
                    logger.warning(f"Unhandled status: {status}")
                    last_check_time = current_time
                    continue

            except Exception as e:
                logger.error(f"Error checking download status: {str(e)}")
                return DownloadCachedStreamResult(success=False, error=f"Error checking status: {str(e)}")

            time.sleep(self.STATUS_CHECK_INTERVAL)

    def _add_active_download(self, content_id: str, torrent_id: str):
        """Add a download to active downloads tracking."""
        self.active_downloads[content_id].add(torrent_id)
        logger.debug(f"Added download {torrent_id} to content {content_id} tracking")

    def _remove_active_download(self, content_id: str, torrent_id: str):
        """Remove a download from active downloads tracking."""
        if content_id in self.active_downloads:
            self.active_downloads[content_id].discard(torrent_id)
            logger.debug(f"Removed download {torrent_id} from content {content_id} tracking")
            if not self.active_downloads[content_id]:
                del self.active_downloads[content_id]
                logger.debug(f"Removed empty content {content_id} from tracking")

    def _mark_content_complete(self, content_id: str):
        """Mark a content as having completed download."""
        self.download_complete[content_id] = True
        logger.debug(f"Marked content {content_id} as complete")

    def _is_content_complete(self, content_id: str) -> bool:
        """Check if content has completed download."""
        is_complete = content_id in self.download_complete and self.download_complete[content_id]
        logger.debug(f"Content {content_id} complete status: {is_complete}")
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

    def add_torrent(self, stream: Stream, attempt: int = 1) -> Optional[Dict]:
        """Add a torrent to Real-Debrid and return the torrent info"""
        if attempt > 5:
            logger.error(f"Failed to add torrent {stream.id} after {attempt-1} attempts")
            return None

        try:
            # Check if we're at the active torrent limit
            if not self._can_start_download():
                logger.debug(f"Active limit exceeded, forcing cleanup (attempt {attempt}/5)")
                if not self._cleanup_if_needed():
                    return None
                return self.add_torrent(stream, attempt + 1)

            # Add the magnet/torrent
            if stream.magnet:
                response = self.api.request_handler.execute(HttpMethod.POST, "torrents/addMagnet", data={"magnet": stream.magnet})
            else:
                response = self.api.request_handler.execute(HttpMethod.PUT, "torrents/addTorrent", data=stream.torrent)

            if not response or not isinstance(response, dict):
                logger.error(f"Invalid response adding torrent {stream.id}: {response}")
                return None

            torrent_id = response.get("id")
            if not torrent_id:
                logger.error(f"No torrent ID in response: {response}")
                return None

            # Get info about the torrent
            info = self.api.request_handler.execute(HttpMethod.GET, f"torrents/info/{torrent_id}")
            if not info or not isinstance(info, dict):
                logger.error(f"Invalid info response for torrent {torrent_id}: {info}")
                return None

            return info

        except RealDebridError as e:
            if "rate limit exceeded" in str(e).lower():
                # Add exponential backoff for rate limits
                wait_time = max(2, min(2 ** attempt, 30))  # Minimum 2 second wait, then exponential backoff up to 30 seconds
                logger.warning(f"Rate limit hit, waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                return self.add_torrent(stream, attempt + 1)
            logger.error(f"Failed to add torrent {stream.id} after {attempt} attempts: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to add torrent {stream.id} after {attempt} attempts: {e}")
            return None

    def _is_active_status(self, status: str) -> bool:
        """Check if a torrent status counts as active."""
        return status in ("downloading", "uploading", "compressing", "magnet_conversion", "waiting_files_selection")

    def _cleanup_inactive_torrents(self) -> int:
        """Clean up inactive, errored, or stalled torrents to free up slots.
        Returns number of torrents cleaned up."""
        
        # Check if enough time has passed since last cleanup
        current_time = datetime.now()
        if (current_time - self.last_cleanup_time).total_seconds() < self.CLEANUP_INTERVAL:
            return 0
            
        try:
            # First check active torrent count
            try:
                active_count = self.api.request_handler.execute(HttpMethod.GET, "torrents/activeCount")
                logger.debug(f"Active torrents: {active_count['nb']}/{active_count['limit']}")
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
                            time_elapsed = (current_time - added_time).total_seconds()
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
            
            logger.debug(f"Using thresholds - Magnet: {magnet_threshold/60:.1f}m, General: {time_threshold/60:.1f}m")
            
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
            
            # Convert to final format and process deletions
            to_delete = [(t[1], t[2]) for t in to_delete]
            
            # Process deletion in batches
            while to_delete:
                batch = to_delete[:batch_size]
                to_delete = to_delete[batch_size:]
                
                for torrent_id, reason in batch:
                    try:
                        self.api.request_handler.execute(HttpMethod.DELETE, f"torrents/delete/{torrent_id}")
                        cleaned += 1
                        logger.info(f"Cleaned up torrent: {reason}")
                    except Exception as e:
                        logger.error(f"Failed to delete torrent {torrent_id}: {e}")
                
                # Small delay between batches
                if to_delete:  # If we have more to process, wait briefly
                    time.sleep(0.5)
            
            self.last_cleanup_time = current_time
            return cleaned
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return 0

    def _get_torrent_info(self, torrent_id: str) -> Optional[Dict]:
        """Get detailed info for a torrent including seeders and status"""
        try:
            info = self._api_request_with_backoff(HttpMethod.GET, f"torrents/info/{torrent_id}")
            if isinstance(info, dict):
                return info
            logger.error(f"Invalid response from torrent info: {info}")
            return None
        except Exception as e:
            logger.error(f"Error getting torrent info: {e}")
            return None

    def _api_request_with_backoff(self, method: HttpMethod, endpoint: str, data: Optional[Dict] = None, max_attempts: int = 3) -> Any:
        """Make API request with exponential backoff for rate limits"""
        attempt = 0
        last_error = None
        
        while attempt < max_attempts:
            try:
                # Wait for rate limiter before making request
                self.rate_limiter.wait_if_needed()
                
                # Make the request
                if data:
                    response = self.api.request_handler.execute(method, endpoint, data=data)
                else:
                    response = self.api.request_handler.execute(method, endpoint)
                return response
                
            except RealDebridError as e:
                last_error = e
                if "rate limit exceeded" in str(e).lower():
                    wait_time = min(2 ** attempt * 2, 30)  # Max 30 second wait
                    logger.debug(f"Rate limit hit, waiting {wait_time}s before retry (attempt {attempt + 1}/{max_attempts})")
                    time.sleep(wait_time)
                    attempt += 1
                    continue
                raise  # Re-raise if not a rate limit error
            except Exception as e:
                last_error = e
                raise
                
        logger.error(f"Failed after {max_attempts} attempts: {last_error}")
        raise last_error

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

            # Group torrents by status - NEVER include downloaded torrents
            non_downloaded = []
            errors = []
            downloaded_count = 0

            for t in torrents:
                if not t.get('id') or t['id'] in exclude_ids:
                    continue

                status = t.get('status', '')
                if status == "downloaded":
                    downloaded_count += 1  # Just count them
                    continue  # Skip downloaded torrents
                elif status in ("error", "virus", "dead", "magnet_error"):
                    errors.append(t)
                else:
                    non_downloaded.append(t)

            logger.debug(
                f"Torrent status summary:\n"
                f"- {len(non_downloaded)} non-downloaded to clean\n"
                f"- {len(errors)} errored to clean\n"
                f"- {downloaded_count} downloaded (preserved)\n"
                f"(excluding {len(exclude_ids)} torrents)"
            )

            # Process in priority order: errors -> non-downloaded
            all_to_clean = errors + non_downloaded

            # Process in small batches
            batch_size = 2
            for i in range(0, len(all_to_clean), batch_size):
                batch = all_to_clean[i:i + batch_size]

                for torrent in batch:
                    torrent_id = torrent['id']
                    if torrent_id in cleaned_ids:
                        continue

                    try:
                        # Delete associated downloads first
                        for link in torrent.get("links", []):
                            try:
                                download_id = link.split("/")[-1]
                                self._api_request_with_backoff(
                                    HttpMethod.DELETE,
                                    f"downloads/delete/{download_id}",
                                    max_attempts=3
                                )
                                logger.debug(f"Deleted download {download_id} for torrent {torrent_id}")
                                time.sleep(1)
                            except Exception as e:
                                if "404" not in str(e):
                                    logger.warning(f"Failed to delete download {download_id}: {e}")

                        # Delete the torrent
                        self._api_request_with_backoff(
                            HttpMethod.DELETE,
                            f"torrents/delete/{torrent_id}",
                            max_attempts=3
                        )
                        cleaned_ids.add(torrent_id)
                        logger.debug(f"Deleted torrent {torrent_id} (status: {torrent.get('status')})")

                    except Exception as e:
                        if "404" not in str(e):
                            logger.warning(f"Failed to clean torrent {torrent_id}: {e}")

                # Add delay between batches
                if i + batch_size < len(all_to_clean):
                    time.sleep(3)

            return True

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return False

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
                f"Status: {status}, Progress: {progress}%, Speed: {speed_mb:.2f}MB/s, Seeders: {seeders}"
            )
            
            # Log file details if available
            if files:
                logger.debug("Available files:")
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
        try:
            # Get active torrent count from API
            active_count = self.api.request_handler.execute(HttpMethod.GET, "torrents/activeCount")
            if not isinstance(active_count, dict):
                logger.error(f"Invalid response from activeCount: {active_count}")
                return False
                
            current_count = active_count.get("nb", 0)
            limit = active_count.get("limit", 10)  # Default RealDebrid limit is 10
            
            if current_count >= limit:
                logger.debug(f"At active torrent limit ({current_count}/{limit})")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to check active torrent count: {e}")
            return False

    def _add_magnet_or_torrent(self, stream: Stream, attempt: int = 1) -> Optional[Dict]:
        """Add a magnet or torrent to Real-Debrid and return the torrent info"""
        if attempt > 5:
            logger.error(f"Failed to add torrent {stream.id} after {attempt-1} attempts")
            return None

        try:
            # Respect rate limits
            self.rate_limiter.wait_if_needed()

            # Construct magnet link from infohash
            magnet = f"magnet:?xt=urn:btih:{stream.infohash}"
            
            # Add the magnet
            response = self.api.request_handler.execute(
                HttpMethod.POST, 
                "torrents/addMagnet", 
                data={"magnet": magnet}
            )

            if not response or not isinstance(response, dict):
                logger.error(f"Invalid response adding torrent {stream.id}: {response}")
                return None

            torrent_id = response.get("id")
            if not torrent_id:
                logger.error(f"No torrent ID in response: {response}")
                return None

            # Get info about the torrent
            self.rate_limiter.wait_if_needed()  # Wait before making another API call
            info = self.api.request_handler.execute(
                HttpMethod.GET, 
                f"torrents/info/{torrent_id}"
            )
            if not info or not isinstance(info, dict):
                logger.error(f"Invalid info response for torrent {torrent_id}: {info}")
                return None

            return info

        except RealDebridError as e:
            if "rate limit exceeded" in str(e).lower():
                # Exponential backoff with jitter
                base_wait = min(30, 2 ** attempt)  # Cap at 30 seconds
                jitter = random.uniform(0, 0.1 * base_wait)  # Add up to 10% jitter
                wait_time = base_wait + jitter
                logger.warning(f"Rate limit hit, waiting {wait_time:.1f} seconds before retry...")
                time.sleep(wait_time)
                return self._add_magnet_or_torrent(stream, attempt + 1)
            logger.error(f"Failed to add torrent {stream.id} after {attempt} attempts: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to add torrent {stream.id} after {attempt} attempts: {e}")
            return None

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