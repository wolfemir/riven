import os
import pathlib

from babelfish import Language
from loguru import logger
from subliminal import ProviderPool, Video, region, save_subtitles
from subliminal.exceptions import AuthenticationError

from program.media.subtitle import Subtitle
from program.settings.manager import settings_manager
from program.utils import root_dir

from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache


class Subliminal:
    def __init__(self):
        self.key = "subliminal"
        self.settings = settings_manager.settings.post_processing.subliminal
        self.initialized = False
        self._enabled = self.settings.enabled
        
        if not self._enabled:
            return

        # Configure region with better caching
        if not region.is_configured:
            region.configure("dogpile.cache.dbm", 
                           arguments={
                               "filename": f"{root_dir}/data/subliminal.dbm",
                               "lock_timeout": 30
                           })

        self.providers = ["gestdown", "opensubtitles", "opensubtitlescom", "podnapisi", "tvsubtitles"]
        self.provider_config = {}
        self.pool = ProviderPool(providers=self.providers)

        for provider, value in self.settings.providers.items():
            if value["enabled"]:
                self.provider_config[provider] = {
                    "username": value["username"],
                    "password": value["password"]
                }
        
        self.pool.provider_configs = self.provider_config

        # Create thread pool for parallel provider initialization
        with ThreadPoolExecutor(max_workers=len(self.providers)) as executor:
            futures = []
            for provider in self.providers:
                futures.append(executor.submit(self._initialize_provider, provider))
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.warning(f"Provider initialization failed: {str(e)}")

        self.languages = set(create_language_from_string(lang) for lang in self.settings.languages)
        self.subtitle_cache = {}
        self._max_workers = min(10, len(self.languages) * 2)  # Optimize worker count
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        self.initialized = True

    @property
    def enabled(self):
        return self._enabled

    def _initialize_provider(self, provider):
        """Initialize a single provider with error handling."""
        try:
            self.pool[provider].initialize()
            if self.provider_config.get(provider):
                if provider == "opensubtitlescom":
                    self.pool[provider].login()
                    if not self.pool[provider].check_token():
                        raise AuthenticationError
        except Exception as e:
            logger.warning(f"Could not initialize provider: {provider}. Error: {str(e)}")
            if provider == "opensubtitlescom":
                self.pool.initialized_providers.pop(provider, None)
                self.provider_config.pop(provider, None)
                self.pool[provider].initialize()
                logger.warning("Using default opensubtitles.com provider.")
            raise

    @lru_cache(maxsize=128)
    def _get_video_info(self, real_name):
        """Cache video information parsing."""
        return Video.fromname(real_name)

    def get_subtitles(self, item):
        if item.type not in ["movie", "episode"]:
            return {}

        real_name = pathlib.Path(item.symlink_path).resolve().name
        try:
            video = self._get_video_info(real_name)
            video.symlink_path = item.symlink_path
            video.subtitle_languages = get_existing_subtitles(
                pathlib.Path(item.symlink_path).stem,
                pathlib.Path(item.symlink_path).parent
            )

            # Parallel subtitle listing and downloading
            futures = []
            with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                for language in self.languages:
                    if language not in video.subtitle_languages:
                        futures.append(
                            executor.submit(
                                self.pool.list_subtitles,
                                video,
                                {language}
                            )
                        )

            all_subtitles = []
            for future in as_completed(futures):
                try:
                    subtitles = future.result()
                    if subtitles:
                        all_subtitles.extend(subtitles)
                except Exception as e:
                    logger.error(f"Error listing subtitles: {str(e)}")

            if all_subtitles:
                return video, self.pool.download_best_subtitles(all_subtitles, video, self.languages)
            return video, []

        except ValueError:
            logger.error(f"Could not parse video name: {real_name}")
        return {}

    def save_subtitles(self, video, subtitles, item):
        """Save subtitles with parallel processing."""
        if not subtitles:
            return

        futures = []
        with ThreadPoolExecutor(max_workers=min(len(subtitles), 5)) as executor:
            for subtitle in subtitles:
                original_name = video.name
                video.name = pathlib.Path(video.symlink_path)
                futures.append(
                    executor.submit(
                        self._save_single_subtitle,
                        video,
                        subtitle,
                        item,
                        original_name
                    )
                )

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error saving subtitle: {str(e)}")

    def _save_single_subtitle(self, video, subtitle, item, original_name):
        """Save a single subtitle with proper cleanup."""
        try:
            saved = save_subtitles(video, [subtitle])
            for saved_sub in saved:
                logger.info(f"Downloaded ({saved_sub.language}) subtitle for {pathlib.Path(item.symlink_path).stem}")
        finally:
            video.name = original_name

    def scan_files_and_download(self):
        # Do we want this?
        pass
        # videos = _scan_videos(settings_manager.settings.symlink.library_path)
        # subtitles = download_best_subtitles(videos, {Language("eng")})
        # for video, subtitle in subtitles.items():
        #     original_name = video.name
        #     video.name = pathlib.Path(video.symlink)
        #     saved = save_subtitles(video, subtitle)
        #     video.name = original_name
        #     for subtitle in saved:
        #         logger.info(f"Downloaded ({subtitle.language}) subtitle for {pathlib.Path(video.symlink).stem}")

    def run(self, item):
        for language in self.languages:
            key = str(language)
            item.subtitles.append(Subtitle({key: None}))
        try:
            video, subtitles = self.get_subtitles(item)
            self.save_subtitles(video, subtitles, item)
            self.update_item(item)
        except Exception as e:
            logger.error(f"Failed to download subtitles for {item.log_string}: {e}")

    def update_item(self, item):
        folder = pathlib.Path(item.symlink_path).parent
        subs = get_existing_subtitles(pathlib.Path(item.symlink_path).stem, folder)
        for lang in subs:
            key = str(lang)
            for subtitle in item.subtitles:
                if subtitle.language == key:
                    subtitle.file = (folder / lang.file).__str__()
                    break

    @staticmethod
    def should_submit(item):
        return item.type in ["movie", "episode"] and not any(subtitle.file is not None for subtitle in item.subtitles)


def _scan_videos(directory):
    """
    Scan the given directory recursively for video files.

    :param directory: Path to the directory to scan
    :return: List of Video objects
    """
    videos = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith((".mp4", ".mkv", ".avi", ".mov", ".wmv")):
                video_path = os.path.join(root, file)
                video_name = pathlib.Path(video_path).resolve().name
                video = Video.fromname(video_name)
                video.symlink = pathlib.Path(video_path)

                # Scan for subtitle files
                video.subtitle_languages = get_existing_subtitles(video.symlink.stem, pathlib.Path(root))
                videos.append(video)
    return videos


def create_language_from_string(lang: str) -> Language:
    try:
        if len(lang) == 2:
            return Language.fromcode(lang, "alpha2")
        if len(lang) == 3:
            return Language.fromcode(lang, "alpha3b")
    except ValueError:
        logger.error(f"Invalid language code: {lang}")
        return None


def get_existing_subtitles(filename: str, path: pathlib.Path) -> set[Language]:
    subtitle_languages = set()
    for file in path.iterdir():
        if file.stem.startswith(filename) and file.suffix == ".srt":
                parts = file.name.split(".")
                if len(parts) > 2:
                    lang_code = parts[-2]
                    language = create_language_from_string(lang_code)
                    language.file = file.name
                    subtitle_languages.add(language)
    return subtitle_languages