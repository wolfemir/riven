import os
import random
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Union
import re
import time

from loguru import logger
from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from program.db.db import db
from program.types import Event, EventType
from program.media.item import Episode, MediaItem, Movie, Season, Show
from program.media.state import States
from program.settings.manager import settings_manager


class Symlinker:
    """
    A class that represents a symlinker thread.

    Settings Attributes:
        rclone_path (str): The absolute path of the rclone mount root directory.
        library_path (str): The absolute path of the location we will create our symlinks that point to the rclone_path.
    """

    def __init__(self):
        self.key = "symlink"
        self.settings = settings_manager.settings.symlink
        self.rclone_path = self.settings.rclone_path
        self.initialized = self.validate()
        if not self.initialized:
            return
        logger.info(f"Rclone path symlinks are pointed to: {self.rclone_path}")
        logger.info(f"Symlinks will be placed in: {self.settings.library_path}")
        logger.success("Symlink initialized!")

    def validate(self):
        """Validate paths and create the initial folders."""
        library_path = self.settings.library_path
        if not self.rclone_path or not library_path:
            logger.error("rclone_path or library_path not provided.")
            return False
        if self.rclone_path == Path(".") or library_path == Path("."):
            logger.error("rclone_path or library_path is set to the current directory.")
            return False
        if not self.rclone_path.exists():
            logger.error(f"rclone_path does not exist: {self.rclone_path}")
            return False
        if not library_path.exists():
            logger.error(f"library_path does not exist: {library_path}")
            return False
        if not self.rclone_path.is_absolute():
            logger.error(f"rclone_path is not an absolute path: {self.rclone_path}")
            return False
        if not library_path.is_absolute():
            logger.error(f"library_path is not an absolute path: {library_path}")
            return False
        return self._create_initial_folders()

    def _create_initial_folders(self):
        """Create the initial library folders."""
        try:
            self.library_path_movies = self.settings.library_path / "movies"
            self.library_path_shows = self.settings.library_path / "shows"
            self.library_path_anime_movies = self.settings.library_path / "anime_movies"
            self.library_path_anime_shows = self.settings.library_path / "anime_shows"
            folders = [
                self.library_path_movies,
                self.library_path_shows,
                self.library_path_anime_movies,
                self.library_path_anime_shows,
            ]
            for folder in folders:
                if not folder.exists():
                    folder.mkdir(parents=True, exist_ok=True)
        except FileNotFoundError as e:
            logger.error(f"Path not found when creating directory: {e}")
            return False
        except PermissionError as e:
            logger.error(f"Permission denied when creating directory: {e}")
            return False
        except OSError as e:
            logger.error(f"OS error when creating directory: {e}")
            return False
        return True

    def run(self, item: Union[Movie, Show, Season, Episode]):
        """Check if the media item exists and create a symlink if it does"""
        items = self._get_items_to_update(item)
        if not self._should_submit(items):
            if item.symlinked_times == 5:
                logger.debug(f"Soft resetting {item.log_string} because required files were not found")
                item.blacklist_active_stream()
                item.reset()
                yield item
            next_attempt = self._calculate_next_attempt(item)
            logger.debug(f"Waiting for {item.log_string} to become available, next attempt in {round((next_attempt - datetime.now()).total_seconds())} seconds")
            item.symlinked_times += 1
            yield (item, next_attempt)
        try:
            for _item in items:
                self._symlink(_item)
            logger.log("SYMLINKER", f"Symlinks created for {item.log_string}")
        except Exception as e:
            logger.error(f"Exception thrown when creating symlink for {item.log_string}: {e}")
        yield item

    def _calculate_next_attempt(self, item: Union[Movie, Show, Season, Episode]) -> datetime:
        """Calculate next retry attempt time using fixed delays."""
        delays = self.settings.retry_delays
        attempt = min(item.symlinked_times, len(delays) - 1)
        delay = timedelta(seconds=delays[attempt])
        return datetime.now() + delay

    def _should_submit(self, items: Union[Movie, Show, Season, Episode]) -> bool:
        """Check if the item should be submitted for symlink creation."""
        random_item = random.choice(items)
        if not _get_item_path(random_item):
            return False
        else:
            return True

    def _get_items_to_update(self, item: Union[Movie, Show, Season, Episode]) -> List[Union[Movie, Episode]]:
        items = []
        if item.type in ["episode", "movie"]:
            items.append(item)
            item.set("folder", item.folder)
        elif item.type == "show":
            for season in item.seasons:
                for episode in season.episodes:
                    if episode.state == States.Downloaded:
                        items.append(episode)
        elif item.type == "season":
            for episode in item.episodes:
                if episode.state == States.Downloaded:
                    items.append(episode)
        return items

    def symlink(self, item: Union[Movie, Episode]) -> bool:
        """Create a symlink for the given media item if it does not already exist."""
        return self._symlink(item)

    def _symlink(self, item: Union[Movie, Episode]) -> bool:
        """Create a symlink for the given media item if it does not already exist."""
        if not item:
            logger.error(f"Invalid item sent to Symlinker: {item}")
            return False

        source = _get_item_path(item)
        if not source:
            logger.error(f"Could not find path for {item.log_string}, cannot create symlink.")
            return False

        filename = self._determine_file_name(item)
        if not filename:
            logger.error(f"Symlink filename is None for {item.log_string}, cannot create symlink.")
            return False

        extension = os.path.splitext(item.file)[1][1:]
        symlink_filename = f"{filename}.{extension}"
        destination = self._create_item_folders(item, symlink_filename)

        try:
            # Create parent directories if they don't exist
            os.makedirs(os.path.dirname(destination), exist_ok=True)

            # Create temporary symlink with unique suffix
            temp_link = f"{destination}.tmp"
            if os.path.exists(temp_link) or os.path.islink(temp_link):
                os.remove(temp_link)

            # Create the new symlink as temporary first
            os.symlink(source, temp_link)

            # Verify the temporary symlink
            if not os.path.exists(temp_link):
                logger.error(f"Failed to create temporary symlink at {temp_link} for {item.log_string}")
                return False

            actual_target = os.path.realpath(temp_link)
            expected_target = os.path.realpath(source)
            if actual_target != expected_target:
                logger.error(f"Symlink validation failed: {temp_link} points to {actual_target} instead of {expected_target}")
                os.remove(temp_link)
                return False

            # Only after successful creation and validation, replace the old symlink
            if os.path.exists(destination) or os.path.islink(destination):
                os.remove(destination)

            # Atomic rename of temporary symlink to final destination
            os.rename(temp_link, destination)

            # Update item attributes
            item.set("symlinked", True)
            item.set("symlinked_at", datetime.now())
            item.set("symlinked_times", item.symlinked_times + 1)
            item.set("symlink_path", destination)
            logger.debug(f"Successfully created symlink for {item.log_string} at {destination}")
            return True

        except PermissionError as e:
            logger.error(f"Permission denied when creating symlink for {item.log_string}: {e}")
        except OSError as e:
            if e.errno == 36:
                logger.error(f"Filename too long when creating symlink for {item.log_string}: {e}")
            else:
                logger.error(f"OS error when creating symlink for {item.log_string}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating symlink for {item.log_string}: {e}")
        
        # Clean up temporary symlink if it exists after any error
        if os.path.exists(temp_link) or os.path.islink(temp_link):
            try:
                os.remove(temp_link)
            except Exception as e:
                logger.error(f"Failed to clean up temporary symlink {temp_link}: {e}")
        
        return False

    def _create_item_folders(self, item: Union[Movie, Show, Season, Episode], filename: str) -> str:
        """Create necessary folders and determine the destination path for symlinks."""
        is_anime: bool = hasattr(item, "is_anime") and item.is_anime

        movie_path: Path = self.library_path_movies
        show_path: Path = self.library_path_shows

        if self.settings.separate_anime_dirs and is_anime:
            if isinstance(item, Movie):
                movie_path = self.library_path_anime_movies
            elif isinstance(item, (Show, Season, Episode)):
                show_path = self.library_path_anime_shows

        def create_folder_path(base_path, *subfolders):
            path = os.path.join(base_path, *subfolders)
            os.makedirs(path, exist_ok=True)
            return path

        if isinstance(item, Movie):
            movie_folder = f"{item.title.replace('/', '-')} ({item.aired_at.year}) {{imdb-{item.imdb_id}}}"
            destination_folder = create_folder_path(movie_path, movie_folder)
            item.set("update_folder", destination_folder)
        elif isinstance(item, Show):
            folder_name_show = f"{item.title.replace('/', '-')} ({item.aired_at.year}) {{imdb-{item.imdb_id}}}"
            destination_folder = create_folder_path(show_path, folder_name_show)
            item.set("update_folder", destination_folder)
        elif isinstance(item, Season):
            show = item.parent
            folder_name_show = f"{show.title.replace('/', '-')} ({show.aired_at.year}) {{imdb-{show.imdb_id}}}"
            show_path = create_folder_path(show_path, folder_name_show)
            folder_season_name = f"Season {str(item.number).zfill(2)}"
            destination_folder = create_folder_path(show_path, folder_season_name)
            item.set("update_folder", destination_folder)
        elif isinstance(item, Episode):
            show = item.parent.parent
            folder_name_show = f"{show.title.replace('/', '-')} ({show.aired_at.year}) {{imdb-{show.imdb_id}}}"
            show_path = create_folder_path(show_path, folder_name_show)
            season = item.parent
            folder_season_name = f"Season {str(season.number).zfill(2)}"
            destination_folder = create_folder_path(show_path, folder_season_name)
            item.set("update_folder", destination_folder)

        return os.path.join(destination_folder, filename.replace("/", "-"))

    def _determine_file_name(self, item: Union[Movie, Episode]) -> str | None:
        """Determine the filename of the symlink."""
        filename = None
        if isinstance(item, Movie):
            filename = f"{item.title} ({item.aired_at.year}) " + "{imdb-" + item.imdb_id + "}"
        elif isinstance(item, Season):
            showname = item.parent.title
            showyear = item.parent.aired_at.year
            filename = f"{showname} ({showyear}) - Season {str(item.number).zfill(2)}"
        elif isinstance(item, Episode):
            episode_string = ""
            episode_number: List[int] = item.get_file_episodes()
            if episode_number and item.number in episode_number:
                if len(episode_number) > 1:
                    episode_string = f"e{str(episode_number[0]).zfill(2)}-e{str(episode_number[-1]).zfill(2)}"
                else:
                    episode_string = f"e{str(item.number).zfill(2)}"
            if episode_string != "":
                showname = item.parent.parent.title
                showyear = item.parent.parent.aired_at.year
                filename = f"{showname} ({showyear}) - s{str(item.parent.number).zfill(2)}{episode_string}"
        return filename

    def delete_item_symlinks(self, item: "MediaItem") -> bool:
        """Delete symlinks and directories based on the item type."""
        if not isinstance(item, (Movie, Show)):
            logger.debug(f"skipping delete symlink for {item.log_string}: Not a movie or show")
            return False
        item_path = None
        if isinstance(item, Show):
            base_path = self.library_path_anime_shows if item.is_anime else self.library_path_shows
            item_path = base_path / f"{item.title.replace('/', '-')} ({item.aired_at.year}) {{imdb-{item.imdb_id}}}"
        elif isinstance(item, Movie):
            base_path = self.library_path_anime_movies if item.is_anime else self.library_path_movies
            item_path = base_path / f"{item.title.replace('/', '-')} ({item.aired_at.year}) {{imdb-{item.imdb_id}}}"
        return _delete_symlink(item, item_path)

    def delete_item_symlinks_by_id(self, item_id: int) -> bool:
        """Delete symlinks and directories based on the item ID."""
        with db.Session() as session:
            item = session.execute(select(MediaItem).where(MediaItem.id == item_id)).unique().scalar_one()
            if not item:
                logger.error(f"No item found with ID {item_id}")
                return False
            return self.delete_item_symlinks(item)

    def check_and_requeue_broken_symlinks(self) -> None:
        """Check for broken symlinks in library paths and requeue them for processing."""
        logger.info("Starting broken symlink check...")
        
        paths_to_check = [
            self.library_path_movies,
            self.library_path_shows,
            self.library_path_anime_movies,
            self.library_path_anime_shows
        ]
        
        broken_links = []
        for base_path in paths_to_check:
            if not base_path.exists():
                continue
                
            # Walk through all directories
            for root, _, files in os.walk(base_path):
                for filename in files:
                    full_path = Path(root) / filename
                    if full_path.is_symlink():
                        try:
                            # Check if symlink is broken
                            if not os.path.exists(os.path.realpath(full_path)):
                                broken_links.append(full_path)
                                logger.warning(f"Found broken symlink: {full_path}")
                        except Exception as e:
                            logger.error(f"Error checking symlink {full_path}: {e}")
                            broken_links.append(full_path)

        if not broken_links:
            logger.info("No broken symlinks found.")
            return

        logger.info(f"Found {len(broken_links)} broken symlinks. Checking database and requeueing...")

        with db.Session() as session:
            for link_path in broken_links:
                try:
                    # Extract item information from the symlink path
                    relative_path = str(link_path.relative_to(self.library_path_movies if 'movies' in str(link_path) else self.library_path_shows))
                    
                    # Try to find the item in the database
                    query = None
                    if 'movies' in str(link_path):
                        # Extract movie title and year from filename
                        match = re.match(r"(.+?)\s*\((\d{4})\)\s*{imdb-([^}]+)}", link_path.parent.name)
                        if match:
                            title, year, imdb_id = match.groups()
                            query = select(Movie).where(Movie.imdb_id == imdb_id)
                    else:
                        # For TV shows, extract show title and year
                        show_dir = None
                        for parent in link_path.parents:
                            if match := re.match(r"(.+?)\s*\((\d{4})\)\s*{imdb-([^}]+)}", parent.name):
                                show_dir = parent
                                break
                        
                        if show_dir and (match := re.match(r"(.+?)\s*\((\d{4})\)\s*{imdb-([^}]+)}", show_dir.name)):
                            title, year, imdb_id = match.groups()
                            query = select(Episode).join(Season).join(Show).where(Show.imdb_id == imdb_id)
                            
                            # If it's an episode, try to extract season and episode numbers
                            if episode_match := re.match(r".+?s(\d{2})e(\d{2})", link_path.stem):
                                season_num, episode_num = map(int, episode_match.groups())
                                query = query.where(
                                    and_(
                                        Season.number == season_num,
                                        Episode.number == episode_num
                                    )
                                )

                    if query:
                        item = session.execute(query).unique().scalar_one_or_none()
                        if item:
                            # Reset symlink status
                            item.set("symlinked", False)
                            item.set("symlink_path", None)
                            
                            # Add to event queue for reprocessing
                            event = Event(
                                event_type=EventType.SYMLINK,
                                media_item_id=item.id,
                                priority=1  # Higher priority for broken link fixes
                            )
                            session.add(event)
                            logger.info(f"Requeued {item.log_string} for symlink creation")
                        else:
                            logger.warning(f"Could not find database entry for broken symlink: {link_path}")
                    
                        # Remove the broken symlink
                        if os.path.islink(link_path):
                            os.remove(link_path)
                            logger.debug(f"Removed broken symlink: {link_path}")
                    
                except Exception as e:
                    logger.error(f"Error processing broken symlink {link_path}: {e}")
                    continue
            
            try:
                session.commit()
                logger.info("Successfully committed broken symlink fixes to database")
            except Exception as e:
                logger.error(f"Error committing broken symlink fixes to database: {e}")
                session.rollback()

    def get_items_from_filepath(self, filepath: str) -> Optional[MediaItem]:
        """Extract item information from filepath and find matching database entry."""
        try:
            path = Path(filepath)
            imdb_match = None
            season_num = None
            episode_num = None

            # Search for IMDB ID in parent directories
            for parent in path.parents:
                if match := re.search(r'{imdb-([^}]+)}', parent.name):
                    imdb_match = match.group(1)
                    break

            if not imdb_match:
                logger.debug(f"No IMDB ID found in path: {filepath}")
                return None

            # Extract season and episode numbers for TV shows
            if 'Season' in filepath:
                # Try different episode naming patterns
                patterns = [
                    r's(\d{1,2})e(\d{1,2}(?:-e\d{1,2})?)',  # s01e01 or s01e01-e02
                    r'[Ss]eason\s*(\d{1,2}).*?[Ee]pisode\s*(\d{1,2})',  # Season 01 Episode 01
                    r'[. _-](\d{1,2})x(\d{1,2})[. _-]',  # 1x01
                    r'- s(\d{2})e(\d{2})',  # - s01e01
                ]
                
                for pattern in patterns:
                    if match := re.search(pattern, str(path), re.IGNORECASE):
                        season_num = int(match.group(1))
                        episode_num = int(match.group(2).split('-')[0])  # Take first episode number if range
                        break

            with db.Session() as session:
                if season_num is not None and episode_num is not None:
                    # TV Show episode
                    query = (
                        select(Episode)
                        .join(Season)
                        .join(Show)
                        .where(
                            Show.imdb_id == imdb_match,
                            Season.number == season_num,
                            Episode.number == episode_num
                        )
                    )
                    item = session.execute(query).scalar_one_or_none()
                    if item:
                        return item
                    else:
                        logger.debug(f"No episode found for {imdb_match} S{season_num:02d}E{episode_num:02d}")
                else:
                    # Movie
                    query = select(Movie).where(Movie.imdb_id == imdb_match)
                    item = session.execute(query).scalar_one_or_none()
                    if item:
                        return item
                    else:
                        logger.debug(f"No movie found for IMDB ID: {imdb_match}")

        except Exception as e:
            logger.error(f"Error extracting item from filepath {filepath}: {e}")

        return None

    def check_and_fix_symlink(self, symlink_path: str, max_attempts: int = 5, initial_wait: int = 5) -> bool:
        """Check if a symlink is broken and attempt to fix it."""
        try:
            symlink_path = Path(symlink_path)
            if not symlink_path.is_symlink():
                return True  # Not a symlink, nothing to fix

            # Check if target exists
            target = Path(os.readlink(symlink_path))
            if target.exists():
                return True  # Symlink is good

            # Get the item from database
            item = self.get_items_from_filepath(str(symlink_path))
            if not item:
                logger.warning(f"Could not find item in database for path: {symlink_path}")
                return False

            # Try to find the correct file path
            wait_time = initial_wait
            attempts_left = max_attempts
            
            while attempts_left > 0:
                source = _get_item_path(item)
                if source and Path(source).exists():
                    # Remove old symlink and create new one
                    try:
                        symlink_path.unlink()
                        os.symlink(source, symlink_path)
                        logger.info(f"Fixed symlink for {item.log_string} at {symlink_path}")
                        return True
                    except Exception as e:
                        logger.error(f"Failed to create new symlink for {item.log_string}: {e}")
                        return False

                logger.debug(f"File {item.file} not found in rclone_path, waiting {wait_time} seconds. {attempts_left} attempts left.")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff
                attempts_left -= 1

            # If we couldn't find the file, check for existing torrents or requeue for scraping
            with db.Session() as session:
                try:
                    # Refresh item in current session
                    session.add(item)
                    session.refresh(item)

                    # Check if there are any existing torrents
                    existing_torrents = session.execute(
                        select(Torrent)
                        .where(
                            and_(
                                Torrent.media_item_id == item.id,
                                Torrent.state != States.FAILED
                            )
                        )
                    ).scalars().all()

                    if existing_torrents:
                        logger.info(f"Found {len(existing_torrents)} existing torrents for {item.log_string}, checking status...")
                        # Add download event if any torrent is ready
                        for torrent in existing_torrents:
                            if torrent.state in [States.READY, States.DOWNLOADED]:
                                event = Event(
                                    event_type=EventType.DOWNLOAD,
                                    media_item_id=item.id,
                                    priority=2  # Higher priority for missing files
                                )
                                session.add(event)
                                logger.info(f"Added download event for existing torrent of {item.log_string}")
                                break
                    else:
                        # No valid torrents found, queue for scraping
                        logger.info(f"No valid torrents found for {item.log_string}, queueing for scraping")
                        # Reset item state to allow re-scraping
                        item.set("state", States.PENDING)
                        item.set("scraped", False)
                        item.set("scraped_at", None)
                        
                        # Add scrape event
                        event = Event(
                            event_type=EventType.SCRAPE,
                            media_item_id=item.id,
                            priority=2  # Higher priority for missing files
                        )
                        session.add(event)

                    session.commit()
                    logger.info(f"Successfully queued {item.log_string} for processing")

                except Exception as e:
                    logger.error(f"Error queueing {item.log_string} for processing: {e}")
                    session.rollback()
                    return False

            logger.error(f"Failed to find source file for {item.log_string} after {max_attempts} attempts")
            return False

        except Exception as e:
            logger.error(f"Error checking/fixing symlink {symlink_path}: {e}")
            return False

    def process_directory(self, directory: str) -> None:
        """Process a directory recursively to find and fix broken symlinks."""
        try:
            broken_links = []
            for root, _, files in os.walk(directory):
                for filename in files:
                    filepath = Path(root) / filename
                    if filepath.is_symlink() and not os.path.exists(os.readlink(filepath)):
                        broken_links.append(filepath)

            if broken_links:
                logger.info(f"Found {len(broken_links)} broken symlinks in {directory}")
                for link in broken_links:
                    self.check_and_fix_symlink(str(link))
            else:
                logger.info(f"No broken symlinks found in {directory}")

        except Exception as e:
            logger.error(f"Error processing directory {directory}: {e}")

def _delete_symlink(item: Union[Movie, Show], item_path: Path) -> bool:
    try:
        if item_path.exists():
            shutil.rmtree(item_path)
            logger.debug(f"Deleted symlink Directory for {item.log_string}")
            return True
        else:
            logger.debug(f"Symlink Directory for {item.log_string} does not exist, skipping symlink deletion")
            return True
    except FileNotFoundError as e:
        logger.error(f"File not found error when deleting symlink for {item.log_string}: {e}")
    except PermissionError as e:
        logger.error(f"Permission denied when deleting symlink for {item.log_string}: {e}")
    except Exception as e:
        logger.error(f"Failed to delete symlink for {item.log_string}, error: {e}")
    return False

def _get_item_path(item: Union[Movie, Episode]) -> Optional[str]:
    """Get the full path to the item's file in the rclone mount."""
    if not item or not item.file:
        return None

    # First try the direct path from the item's folder
    direct_path = str(Path(settings_manager.settings.symlink.rclone_path) / item.folder / item.file)
    if os.path.exists(direct_path):
        return direct_path

    # Try alternative folder if available
    if hasattr(item, "alternative_folder") and item.alternative_folder:
        alt_path = str(Path(settings_manager.settings.symlink.rclone_path) / item.alternative_folder / item.file)
        if os.path.exists(alt_path):
            return alt_path

    # Search recursively in rclone path for the file
    for root, _, files in os.walk(settings_manager.settings.symlink.rclone_path):
        if item.file in files:
            full_path = str(Path(root) / item.file)
            # Update item's folder to reflect the actual location
            item.folder = os.path.basename(root)
            return full_path

    return None