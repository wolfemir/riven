import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Dict
from sqlalchemy.orm import aliased

from loguru import logger
from sqla_wrapper import Session
from PTT import parse_title

from program.db.db import db
from program.media.subtitle import Subtitle
from program.settings.manager import settings_manager

if TYPE_CHECKING:
    from program.media.item import Episode, MediaItem, Movie, Season, Show

imdbid_pattern = re.compile(r"tt\d+")
season_pattern = re.compile(r"s(\d+)")
episode_pattern = re.compile(r"e(\d+)")

ALLOWED_VIDEO_EXTENSIONS = [
    "mp4",
    "mkv",
    "avi",
    "mov",
    "wmv",
    "flv",
    "m4v",
    "webm",
    "mpg",
    "mpeg",
    "m2ts",
    "ts",
]

MEDIA_DIRS = ["shows", "movies", "anime_shows", "anime_movies"]
POSSIBLE_DIRS = [settings_manager.settings.symlink.library_path / d for d in MEDIA_DIRS]


class SymlinkLibrary:
    def __init__(self):
        self.key = "symlinklibrary"
        self.settings = settings_manager.settings.symlink
        self.initialized = self.validate()
        if not self.initialized:
            logger.error("SymlinkLibrary initialization failed due to invalid configuration.")
            return

    def validate(self) -> bool:
        """Validate the symlink library settings."""
        library_path = Path(self.settings.library_path).resolve()
        if library_path == Path.cwd().resolve():
            logger.error("Library path not set or set to the current directory in SymlinkLibrary settings.")
            return False

        required_dirs: list[str] = ["shows", "movies"]
        if self.settings.separate_anime_dirs:
            required_dirs.extend(["anime_shows", "anime_movies"])
        missing_dirs: list[str] = [d for d in required_dirs if not (library_path / d).exists()]

        if missing_dirs:
            available_dirs: str = ", ".join(os.listdir(library_path))
            logger.error(f"Missing required directories in the library path: {', '.join(missing_dirs)}.")
            logger.debug(f"Library directory contains: {available_dirs}")
            return False
        return True

    def run(self) -> list["MediaItem"]:
        """
        Create a library from the symlink paths. Return stub items that should
        be fed into an Indexer to have the rest of the metadata filled in.
        """
        items = []
        for directory, item_type, is_anime in [("shows", "show", False), ("anime_shows", "anime show", True)]:
            if not self.settings.separate_anime_dirs and is_anime:
                continue
            items.extend(process_shows(self.settings.library_path / directory, item_type, is_anime))

        for directory, item_type, is_anime in [("movies", "movie", False), ("anime_movies", "anime movie", True)]:
            if not self.settings.separate_anime_dirs and is_anime:
                continue
            items.extend(process_items(self.settings.library_path / directory, MediaItem, item_type, is_anime))

        return items

def process_items(directory: Path, item_class, item_type: str, is_anime: bool = False):
    """Process items in the given directory and yield MediaItem instances."""
    items = [
        (Path(root), file)
        for root, _, files in os.walk(directory)
        for file in files
        if os.path.splitext(file)[1][1:] in ALLOWED_VIDEO_EXTENSIONS # Jellyfin/Emby creates extra files
        and Path(root).parent in POSSIBLE_DIRS # MacOS creates extra dirs
    ]
    for path, filename in items:
        if path.parent not in POSSIBLE_DIRS:
            logger.debug(f"Skipping {path.parent} as it's not a valid media directory.")
            continue
        imdb_id = re.search(r"(tt\d+)", filename)
        title = re.search(r"(.+)?( \()", filename)
        if not imdb_id or not title:
            logger.error(f"Can't extract {item_type} imdb_id or title at path {path / filename}")
            continue

        item = item_class({"imdb_id": imdb_id.group(), "title": title.group(1)})
        resolve_symlink_and_set_attrs(item, path / filename)
        find_subtitles(item, path / filename)

        if settings_manager.settings.force_refresh:
            item.set("symlinked", True)
            item.set("update_folder", str(path))
        else:
            item.set("symlinked", True)
            item.set("update_folder", "updated")
        if is_anime:
            item.is_anime = True
        yield item

def resolve_symlink_and_set_attrs(item, path: Path) -> Path:
    # Resolve the symlink path
    resolved_path = (path).resolve()
    item.file = str(resolved_path.stem)
    item.folder = str(resolved_path.parent.stem)
    item.symlink_path = str(path)

def find_subtitles(item, path: Path):
    # Scan for subtitle files
    for file in os.listdir(path.parent):
        if file.startswith(Path(item.symlink_path).stem) and file.endswith(".srt"):
            lang_code = file.split(".")[1]
            item.subtitles.append(Subtitle({lang_code: (path.parent / file).__str__()}))
            logger.debug(f"Found subtitle file {file}.")

def process_shows(directory: Path, item_type: str, is_anime: bool = False) -> Generator["Show", None, None]:
    """Process shows in the given directory and yield Show instances."""
    from program.media.item import Episode, Season, Show  # Import inside function to avoid circular import
    
    for show in os.listdir(directory):
        imdb_id = re.search(r"(tt\d+)", show)
        title = re.search(r"(.+)?( \()", show)
        if not imdb_id or not title:
            logger.log("NOT_FOUND", f"Can't extract {item_type} imdb_id or title at path {directory / show}")
            continue
        show_item = Show({"imdb_id": imdb_id.group(), "title": title.group(1)})
        if is_anime:
            show_item.is_anime = True
        seasons = {}
        for season in os.listdir(directory / show):
            if directory not in POSSIBLE_DIRS:
                logger.debug(f"Skipping {directory} as it's not a valid media directory.")
                continue
            if not (season_number := re.search(r"(\d+)", season)):
                logger.log("NOT_FOUND", f"Can't extract season number at path {directory / show / season}")
                continue
            season_item = Season({"number": int(season_number.group())})
            episodes = {}
            for episode in os.listdir(directory / show / season):
                if os.path.splitext(episode)[1][1:] not in ALLOWED_VIDEO_EXTENSIONS:
                    continue
                episode_numbers: list[int] = parse_title(episode).get("episodes", [])
                if not episode_numbers:
                    logger.log("NOT_FOUND", f"Can't extract episode number at path {directory / show / season / episode}")
                    # Delete the episode since it can't be indexed
                    os.remove(directory / show / season / episode)
                    continue

                for episode_number in episode_numbers:
                    episode_item = Episode({"number": episode_number})
                    resolve_symlink_and_set_attrs(episode_item, Path(directory) / show / season / episode)
                    find_subtitles(episode_item, Path(directory) / show / season / episode)
                    if settings_manager.settings.force_refresh:
                        episode_item.set("symlinked", True)
                        episode_item.set("update_folder", str(Path(directory) / show / season / episode))
                    else:
                        episode_item.set("symlinked", True)
                        episode_item.set("update_folder", "updated")
                    if is_anime:
                        episode_item.is_anime = True
                    episodes[episode_number] = episode_item
            if len(episodes) > 0:
                for i in range(1, max(episodes.keys())+1):
                    season_item.add_episode(episodes.get(i, Episode({"number": i})))
                seasons[int(season_number.group())] = season_item
        if len(seasons) > 0:
            for i in range(1, max(seasons.keys())+1):
                show_item.add_season(seasons.get(i, Season({"number": i})))
        yield show_item


def build_file_map(rclone_path: str, use_cache: bool = True) -> Dict[str, str]:
    """Build a map of filenames to their full paths.
    
    Args:
        rclone_path: Base path to scan for files
        use_cache: Whether to use cached results if available
    """
    cache_key = f"file_map_{rclone_path}"
    cache_timeout = 300  # 5 minutes
    
    if use_cache:
        cached = cache.get(cache_key)
        if cached:
            return cached
    
    file_map = {}
    try:
        for root, _, files in os.walk(rclone_path):
            for filename in files:
                if os.path.splitext(filename)[1][1:] in ALLOWED_VIDEO_EXTENSIONS:
                    full_path = os.path.join(root, filename)
                    # Store both original filename and normalized version
                    file_map[filename] = full_path
                    file_map[normalize_filename(filename)] = full_path
        
        cache.set(cache_key, file_map, timeout=cache_timeout)
        return file_map
        
    except Exception as e:
        logger.error(f"Error building file map for {rclone_path}: {e}")
        return {}

def normalize_filename(filename: str) -> str:
    """Normalize filename for better matching."""
    # Remove common release group tags
    clean = re.sub(r"-[A-Za-z0-9]+$", "", filename)
    # Remove quality tags
    clean = re.sub(r"\b(720p|1080p|2160p|BluRay|WEB-DL|WEBRip|HDRip|BRRip)\b", "", clean)
    # Remove spaces and convert to lowercase
    clean = clean.lower().replace(" ", "")
    return clean

def check_and_fix_symlink(symlink_path, file_map):
    """Check and fix a single symlink with atomic operations and caching."""
    try:
        if isinstance(symlink_path, tuple):
            symlink_path = symlink_path[0]

        target_path = os.readlink(symlink_path)
        filename = os.path.basename(target_path)
        dirname = os.path.dirname(target_path).split("/")[-1]
        
        # Try both exact and normalized filename matches
        correct_path = file_map.get(filename) or file_map.get(normalize_filename(filename))
        
        if correct_path and os.path.exists(correct_path):
            return _fix_symlink(symlink_path, correct_path, filename, dirname)
            
        # Progressive retry with longer delays
        delays = [5, 15, 30, 60, 120]  # 5s, 15s, 30s, 1min, 2min
        attempt = 0
        
        while attempt < len(delays):
            delay = delays[attempt]
            attempts_left = len(delays) - attempt - 1
            
            logger.debug(f"File {filename} not found in rclone_path, waiting {delay} seconds. {attempts_left} attempts left.")
            time.sleep(delay)
            
            # Force refresh file map on retries
            file_map = build_file_map(rclone_path, use_cache=False)
            correct_path = file_map.get(filename) or file_map.get(normalize_filename(filename))
            
            if correct_path and os.path.exists(correct_path):
                return _fix_symlink(symlink_path, correct_path, filename, dirname)
                
            attempt += 1

        # Handle permanent failure
        logger.log("NOT_FOUND", f"Could not find file {filename} in rclone_path after {len(delays)} attempts")
        return _handle_missing_file(symlink_path, filename)

    except Exception as e:
        logger.error(f"Error checking/fixing symlink {symlink_path}: {e}")
        return False

def _handle_missing_file(symlink_path: str, filename: str) -> bool:
    """Handle permanently missing files by cleaning up and updating database."""
    try:
        # Atomic removal of symlink
        if os.path.exists(symlink_path) or os.path.islink(symlink_path):
            os.remove(symlink_path)
            
        with db.Session() as session:
            items = get_items_from_filepath(session, symlink_path)
            if not items:
                return False
                
            for item in items:
                item = session.merge(item)
                # Reset item state
                item.reset()
                item.store_state()
                # Queue for re-scraping
                event = Event(
                    event_type=EventType.SCRAPE,
                    media_item_id=item.id,
                    priority=2  # Higher priority for missing files
                )
                session.add(event)
                session.merge(item)
                
            missing_files += 1
            session.commit()
            logger.info(f"Queued missing file {filename} for re-scraping")
            return False
            
    except Exception as e:
        logger.error(f"Error handling missing file {symlink_path}: {e}")
        return False

def _fix_symlink(symlink_path: str, correct_path: str, filename: str, dirname: str) -> bool:
    """Fix a symlink with atomic operations and proper error handling."""
    temp_link = f"{symlink_path}.tmp"
    
    try:
        with db.Session() as session:
            items = get_items_from_filepath(session, symlink_path)
            if not items:
                logger.log("NOT_FOUND", f"Could not find item in database for path: {symlink_path}")
                return False

            # Create temporary symlink first
            if os.path.exists(temp_link) or os.path.islink(temp_link):
                os.remove(temp_link)
            os.symlink(correct_path, temp_link)
            
            # Verify temporary symlink
            if not os.path.exists(os.path.realpath(temp_link)):
                logger.error(f"Failed to create temporary symlink at {temp_link}")
                return False

            # Remove old symlink if it exists
            if os.path.exists(symlink_path) or os.path.islink(symlink_path):
                os.remove(symlink_path)

            # Atomic rename of temporary to final symlink
            os.rename(temp_link, symlink_path)

            # Update database entries
            for item in items:
                item = session.merge(item)
                item.file = filename
                item.folder = dirname
                item.symlinked = True
                item.symlink_path = correct_path
                item.update_folder = correct_path
                item.store_state()
                session.merge(item)
                logger.log("FILES", f"Fixed symlink for {item.log_string} with path: {correct_path}")

            session.commit()
            return True

    except OSError as e:
        logger.error(f"OS error fixing symlink {symlink_path}: {e}")
        _cleanup_temp_files(temp_link, symlink_path)
        return False

    except Exception as e:
        logger.error(f"Error fixing symlink {symlink_path}: {e}")
        _cleanup_temp_files(temp_link, symlink_path)
        return False

def _cleanup_temp_files(*paths: str) -> None:
    """Clean up temporary files and symlinks."""
    for path in paths:
        if os.path.exists(path) or os.path.islink(path):
            try:
                os.remove(path)
            except Exception as e:
                logger.error(f"Error cleaning up {path}: {e}")

def fix_broken_symlinks(library_path, rclone_path, max_workers=4):
    """Find and fix all broken symlinks in the library path using files from the rclone path."""
    missing_files = 0

    def check_and_fix_symlink(symlink_path, file_map):
        """Check and fix a single symlink."""
        try:
            if isinstance(symlink_path, tuple):
                symlink_path = symlink_path[0]

            target_path = os.readlink(symlink_path)
            filename = os.path.basename(target_path)
            dirname = os.path.dirname(target_path).split("/")[-1]
            
            # First check if file exists in current file_map
            correct_path = file_map.get(filename)
            if correct_path and os.path.exists(correct_path):
                # File found immediately, proceed with fix
                return _fix_symlink(symlink_path, correct_path, filename, dirname)
                
            # Progressive retry with longer delays
            delays = [5, 15, 30, 60, 120]  # 5s, 15s, 30s, 1min, 2min
            attempt = 0
            
            while attempt < len(delays):
                delay = delays[attempt]
                attempts_left = len(delays) - attempt - 1
                
                logger.debug(f"File {filename} not found in rclone_path, waiting {delay} seconds. {attempts_left} attempts left.")
                time.sleep(delay)
                
                # Refresh file map and check again
                file_map = build_file_map(rclone_path)
                correct_path = file_map.get(filename)
                
                if correct_path and os.path.exists(correct_path):
                    return _fix_symlink(symlink_path, correct_path, filename, dirname)
                    
                attempt += 1

            # If we reach here, file was not found after all retries
            logger.log("NOT_FOUND", f"Could not find file {filename} in rclone_path after {len(delays)} attempts")
            
            # Clean up broken symlink and update database
            if os.path.exists(symlink_path) or os.path.islink(symlink_path):
                os.remove(symlink_path)
                
            with db.Session() as session:
                items = get_items_from_filepath(session, symlink_path)
                if items:
                    for item in items:
                        item = session.merge(item)
                        item.reset()
                        item.store_state()
                        session.merge(item)
                    missing_files += 1
                    session.commit()
                    
            return False

        except Exception as e:
            logger.error(f"Error checking/fixing symlink {symlink_path}: {e}")
            return False

    def process_directory(directory, file_map):
        """Process a single directory for broken symlinks."""
        local_broken_symlinks = find_broken_symlinks(directory)
        logger.log("FILES", f"Found {len(local_broken_symlinks)} broken symlinks in {directory}")
        if not local_broken_symlinks:
            return

        with ThreadPoolExecutor(thread_name_prefix="FixSymlinks", max_workers=max_workers) as executor:
            futures = [executor.submit(check_and_fix_symlink, symlink_path, file_map) for symlink_path in local_broken_symlinks]
            for future in as_completed(futures):
                future.result()

    start_time = time.time()
    logger.log("FILES", f"Finding and fixing broken symlinks in {library_path} using files from {rclone_path}")

    file_map = build_file_map(rclone_path)
    if not file_map:
        logger.log("FILES", f"No files found in rclone_path: {rclone_path}. Aborting fix_broken_symlinks.")
        return

    logger.log("FILES", f"Built file map for {rclone_path}")

    top_level_dirs = [os.path.join(library_path, d) for d in os.listdir(library_path) if os.path.isdir(os.path.join(library_path, d))]
    logger.log("FILES", f"Found top-level directories: {top_level_dirs}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_directory, directory, file_map) for directory in top_level_dirs]
        if not futures:
            logger.log("FILES", f"No directories found in {library_path}. Aborting fix_broken_symlinks.")
            return
        for future in as_completed(futures):
            future.result()

    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.log("FILES", f"Finished processing and retargeting broken symlinks. Time taken: {elapsed_time:.2f} seconds.")
    logger.log("FILES", f"Reset {missing_files} items to be rescraped due to missing rclone files.")

def get_items_from_filepath(session: Session, filepath: str) -> list["Movie"] | list["Episode"]:
    """Get items that match the imdb_id or season and episode from a file in library_path"""
    from program.media.item import Episode, Movie, Season, Show  # Import inside function to avoid circular import
    
    items = []
    imdb_id = None

    # Try to find IMDB ID in the path
    match = re.search(r"tt\d{7,8}", filepath)
    if match:
        imdb_id = match.group()

    if not imdb_id:
        logger.debug(f"No IMDB ID found in path: {filepath}")
        return []

    # Check for season/episode numbers
    season_match = re.search(r"s(\d+)", filepath, re.IGNORECASE)
    if season_match:
        season_number = int(season_match.group(1))
        episode_numbers = [int(num) for num in re.findall(r"e(\d+)", filepath, re.IGNORECASE)]
        for ep_num in episode_numbers:
            # Create explicit aliases for Season and Show
            SeasonAlias = aliased(Season, flat=True)
            ShowAlias = aliased(Show, flat=True)
            
            query = (
                session.query(Episode)
                .join(SeasonAlias, Episode.parent_id == SeasonAlias.imdb_id)
                .join(ShowAlias, SeasonAlias.parent_id == ShowAlias.imdb_id)
                .filter(
                    ShowAlias.imdb_id == imdb_id,
                    SeasonAlias.number == season_number,
                    Episode.number == ep_num
                )
            )
            episode_item = query.with_entities(Episode).first()
            if episode_item:
                items.append(episode_item)
    else:
        query = session.query(Movie).filter_by(imdb_id=imdb_id)
        movie_item = query.first()
        if movie_item:
            items.append(movie_item)

    if len(items) > 1:
        logger.log("FILES", f"Found multiple items in database for path: {filepath}")
        for item in items:
            logger.log("FILES", f"Found item: {item.log_string}")
    elif not items:
        logger.debug(f"No items found in database for path: {filepath}")

    return items