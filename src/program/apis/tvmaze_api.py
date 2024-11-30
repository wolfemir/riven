"""TVMaze API client module"""

from datetime import datetime, timedelta
from typing import Optional

from loguru import logger
from requests import Session

from program.media.item import Episode, MediaItem
from program.utils.request import (
    BaseRequestHandler,
    HttpMethod,
    ResponseType,
    create_service_session,
    get_cache_params,
    get_rate_limit_params,
)

class TVMazeAPIError(Exception):
    """Base exception for TVMaze API related errors"""

class TVMazeRequestHandler(BaseRequestHandler):
    def __init__(self, session: Session, response_type=ResponseType.SIMPLE_NAMESPACE, request_logging: bool = False):
        super().__init__(session, response_type=response_type, custom_exception=TVMazeAPIError, request_logging=request_logging)

    def execute(self, method: HttpMethod, endpoint: str, **kwargs):
        return super()._request(method, endpoint, **kwargs)

class TVMazeAPI:
    """Handles TVMaze API communication"""
    BASE_URL = "https://api.tvmaze.com"

    def __init__(self):
        rate_limit_params = get_rate_limit_params(max_calls=20, period=10)
        tvmaze_cache = get_cache_params("tvmaze", 86400)
        session = create_service_session(
            rate_limit_params=rate_limit_params, 
            use_cache=True, 
            cache_params=tvmaze_cache
        )
        self.request_handler = TVMazeRequestHandler(session)

    def get_show_by_imdb_id(self, imdb_id: str) -> Optional[dict]:
        """Get show information by IMDb ID"""
        if not imdb_id:
            return None
        
        url = f"{self.BASE_URL}/lookup/shows"
        response = self.request_handler.execute(
            HttpMethod.GET, 
            url,
            params={"imdb": imdb_id}
        )
        if not response.is_ok:
            return None

        show_url = f"{self.BASE_URL}/shows/{response.data.id}"
        show_response = self.request_handler.execute(HttpMethod.GET, show_url)
        return show_response.data if show_response.is_ok else None

    def get_episode_by_number(self, show_id: int, season: int, episode: int) -> Optional[datetime]:
        """Get episode information by show ID and episode number"""
        if not show_id or not season or not episode:
            return None

        url = f"{self.BASE_URL}/shows/{show_id}/episodebynumber"
        response = self.request_handler.execute(
            HttpMethod.GET,
            url,
            params={
                "season": season,
                "number": episode
            }
        )
        
        if not response.is_ok or not response.data:
            return None

        return self._parse_air_date(response.data)

    def _parse_air_date(self, episode_data) -> Optional[datetime]:
        """Parse episode air date from TVMaze response"""
        if airstamp := getattr(episode_data, "airstamp", None):
            try:
                # Handle both 'Z' suffix and explicit timezone
                timestamp = airstamp.replace('Z', '+00:00')
                if '.' in timestamp:
                    # Strip milliseconds but preserve timezone
                    parts = timestamp.split('.')
                    base = parts[0]
                    tz = parts[1][parts[1].find('+'):]
                    timestamp = base + tz if '+' in parts[1] else base + '+00:00'
                elif not ('+' in timestamp or '-' in timestamp):
                    # Add UTC timezone if none specified
                    timestamp = timestamp + '+00:00'
                return datetime.fromisoformat(timestamp)
            except (ValueError, AttributeError) as e:
                logger.debug(f"Failed to parse TVMaze airstamp: {airstamp} - {e}")

        try:
            if airdate := getattr(episode_data, "airdate", None):
                if airtime := getattr(episode_data, "airtime", None):
                    # Combine date and time with UTC timezone
                    dt_str = f"{airdate}T{airtime}+00:00"
                    return datetime.fromisoformat(dt_str)
                # If we only have a date, set time to midnight UTC
                return datetime.fromisoformat(f"{airdate}T00:00:00+00:00")
        except (ValueError, AttributeError) as e:
            logger.error(f"Failed to parse TVMaze air date/time: {airdate}/{getattr(episode_data, 'airtime', None)} - {e}")
        
        return None

    def get_episode_release_time(self, item: MediaItem) -> Optional[datetime]:
        """Get episode release time for a media item"""
        if not isinstance(item, Episode):
            logger.debug(f"Skipping release time lookup - not an episode: {item.log_string if item else None}")
            return None
            
        if not item.parent or not item.parent.parent:
            logger.debug(f"Skipping release time lookup - missing parent/show: {item.log_string}")
            return None
            
        if not item.parent.parent.imdb_id:
            logger.debug(f"Skipping release time lookup - no IMDb ID: {item.log_string}")
            return None

        show = self.get_show_by_imdb_id(item.parent.parent.imdb_id)
        if not show or not hasattr(show, "id"):
            logger.debug(f"Failed to get TVMaze show for IMDb ID: {item.parent.parent.imdb_id}")
            return None

        # Get episode air date from regular schedule
        air_date = self.get_episode_by_number(show.id, item.parent.number, item.number)
        if air_date:
            logger.debug(f"Found regular schedule time for {item.log_string}: {air_date}")
        
        # Check streaming releases for next 30 days
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        logger.debug(f"Checking streaming schedule for {item.log_string} (Show ID: {show.id})")
        
        for i in range(30):
            check_date = today + timedelta(days=i)
            url = f"{self.BASE_URL}/schedule/web"
            response = self.request_handler.execute(
                HttpMethod.GET, 
                url, 
                params={
                    "date": check_date.strftime("%Y-%m-%d"),
                    "country": None
                }
            )
            
            if not response.is_ok:
                logger.debug(f"Failed to get streaming schedule for {check_date.strftime('%Y-%m-%d')}")
                continue
                
            for release in response.data:
                if not release:
                    continue
                    
                show_data = getattr(release, "show", None)
                if not show_data:
                    continue
                    
                # Get externals safely
                externals = getattr(show_data, "externals", {}) or {}
                show_imdb = externals.get("imdb")
                show_id = getattr(show_data, "id", None)
                
                # Check both IMDb ID and TVMaze show ID
                is_match = (
                    (show_imdb == item.parent.parent.imdb_id or show_id == show.id) and
                    getattr(release, "season", 0) == item.parent.number and
                    getattr(release, "number", 0) == item.number
                )
                
                if is_match:
                    streaming_date = self._parse_air_date(release)
                    if streaming_date:
                        logger.debug(f"Found streaming release for {item.log_string}: {streaming_date}")
                        if not air_date or streaming_date < air_date:
                            air_date = streaming_date
                            logger.debug(f"Using earlier streaming release time for {item.log_string}: {streaming_date}")

        if air_date:
            logger.debug(f"Final release time for {item.log_string}: {air_date}")
        else:
            logger.debug(f"No release time found for {item.log_string}")
        return air_date