import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from loguru import logger

class DownloadManager:
    """Manages download state and progress tracking."""
    
    def __init__(self):
        self.active_downloads: Dict[str, Dict] = {}
        self.completed_downloads: Dict[str, Dict] = {}
        self.failed_downloads: Dict[str, Dict] = {}
        
    def add_download(self, download_id: str, info: Dict) -> None:
        """Add a new download to track."""
        info['start_time'] = datetime.now()
        info['last_update'] = datetime.now()
        self.active_downloads[download_id] = info
        
    def update_download(self, download_id: str, info: Dict) -> None:
        """Update an existing download's info."""
        if download_id in self.active_downloads:
            current_info = self.active_downloads[download_id]
            current_info.update(info)
            current_info['last_update'] = datetime.now()
            
    def complete_download(self, download_id: str) -> None:
        """Mark a download as completed."""
        if download_id in self.active_downloads:
            info = self.active_downloads.pop(download_id)
            info['completion_time'] = datetime.now()
            self.completed_downloads[download_id] = info
            
    def fail_download(self, download_id: str, error: str) -> None:
        """Mark a download as failed."""
        if download_id in self.active_downloads:
            info = self.active_downloads.pop(download_id)
            info['failure_time'] = datetime.now()
            info['error'] = error
            self.failed_downloads[download_id] = info
            
    def get_download_info(self, download_id: str) -> Optional[Dict]:
        """Get info for a specific download."""
        return (self.active_downloads.get(download_id) or 
                self.completed_downloads.get(download_id) or 
                self.failed_downloads.get(download_id))
                
    def get_active_downloads(self) -> List[Dict]:
        """Get list of active downloads."""
        return list(self.active_downloads.values())
        
    def get_completed_downloads(self) -> List[Dict]:
        """Get list of completed downloads."""
        return list(self.completed_downloads.values())
        
    def get_failed_downloads(self) -> List[Dict]:
        """Get list of failed downloads."""
        return list(self.failed_downloads.values())
        
    def cleanup_old_downloads(self, max_age: timedelta) -> None:
        """Remove old completed and failed downloads."""
        now = datetime.now()
        
        # Cleanup completed downloads
        for download_id in list(self.completed_downloads.keys()):
            info = self.completed_downloads[download_id]
            completion_time = info.get('completion_time')
            if completion_time and now - completion_time > max_age:
                del self.completed_downloads[download_id]
                
        # Cleanup failed downloads
        for download_id in list(self.failed_downloads.keys()):
            info = self.failed_downloads[download_id]
            failure_time = info.get('failure_time')
            if failure_time and now - failure_time > max_age:
                del self.failed_downloads[download_id]
                
    def check_stalled_downloads(self, stall_threshold: timedelta) -> List[str]:
        """Check for downloads that haven't been updated recently."""
        now = datetime.now()
        stalled = []
        
        for download_id, info in self.active_downloads.items():
            last_update = info.get('last_update')
            if last_update and now - last_update > stall_threshold:
                stalled.append(download_id)
                
        return stalled
