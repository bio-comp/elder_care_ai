# elder_risk/utils/data_utils.py
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path

import patoolib  # type: ignore[import-untyped]
from loguru import logger


def extract_nested_archives(
    archive_path: str | Path,
    output_dir: str | Path | None = None,
    password: str | None = None,
    max_depth: int = 10,
    extract_callback: Callable[[Path], None] | None = None,
    should_extract: Callable[[Path], bool] | None = None,
    flatten: bool = True,
) -> list[Path]:
    """
    Extract nested archives recursively (supports RAR, ZIP, 7z, and many more).

    Args:
        archive_path: Path to the initial archive file
        output_dir: Directory to extract files to (creates temp dir if None)
        password: Password for encrypted archives (Note: password support depends on backend)
        max_depth: Maximum nesting depth to prevent infinite loops
        extract_callback: Called after each file extraction with the extracted path
        should_extract: Predicate to determine if a file should be extracted
        flatten: If True, extract all files to a flat structure, otherwise preserve nesting

    Returns:
        List of paths to all extracted files
    """
    archive_path = Path(archive_path)
    if not archive_path.exists():
        raise FileNotFoundError(f"Archive file not found: {archive_path}")

    if output_dir is None:
        output_dir = Path(tempfile.mkdtemp(prefix="archive_extract_"))
        logger.info(f"Created temporary output directory: {output_dir}")
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting extraction of {archive_path.name} to {output_dir}")

    # Define recursive helpers as closures to avoid passing unchanged parameters
    def _extract_recursive_flat(archive: Path, current_depth: int) -> list[Path]:
        """Extract archives recursively to a flat directory structure."""

        if current_depth >= max_depth:
            logger.warning(f"Max recursion depth {max_depth} reached. Skipping {archive.name}")
            return []

        logger.debug(f"Extracting {archive.name} at depth {current_depth}")

        # Each archive is extracted to its own temporary directory for isolation
        with tempfile.TemporaryDirectory(prefix=f"{archive.stem}_") as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            extracted_files: list[Path] = []

            try:
                # Use patoolib to extract - it handles everything with one call
                if password and current_depth == 0:  # Only warn once
                    logger.warning("Password provided but may not be supported by all backends")

                patoolib.extract_archive(
                    str(archive),
                    outdir=str(temp_dir),
                    verbosity=-1,  # Suppress patool output since we use loguru
                )
                logger.debug(f"Extracted {archive.name} to temporary directory")

            except Exception as e:
                logger.error(f"Failed to extract archive {archive.name}: {e}")
                return []

            # Process the contents of the temporary directory
            for item in temp_dir.rglob("*"):  # rglob finds files in subdirectories
                if not item.is_file():
                    continue

                # Check if it's an archive and we have depth remaining to recurse
                if _is_archive(item) and current_depth + 1 < max_depth:
                    logger.info(f"Found nested archive: {item.name}")
                    nested_files = _extract_recursive_flat(item, current_depth + 1)
                    extracted_files.extend(nested_files)
                else:
                    # Treat as a regular file (either not an archive or at max depth)
                    if _is_archive(item) and current_depth + 1 >= max_depth:
                        logger.debug(f"Max depth reached, treating {item.name} as regular file")

                    # Move file to destination
                    dest_path = _move_file_to_destination(
                        item, output_dir, should_extract, extract_callback
                    )
                    if dest_path:
                        extracted_files.append(dest_path)

        return extracted_files

    def _extract_recursive_nested(
        archive: Path, target_dir: Path, current_depth: int
    ) -> list[Path]:
        """Extract archives recursively preserving directory structure."""

        if current_depth >= max_depth:
            logger.warning(f"Max recursion depth {max_depth} reached. Skipping {archive.name}")
            return []

        logger.debug(f"Extracting {archive.name} at depth {current_depth}")
        extracted_files: list[Path] = []

        try:
            # Create a subdirectory for this archive's contents
            extract_dir = target_dir / archive.stem
            extract_dir.mkdir(parents=True, exist_ok=True)

            # Use patoolib to extract
            if password and current_depth == 0:  # Only warn once
                logger.warning("Password provided but may not be supported by all backends")

            patoolib.extract_archive(
                str(archive),
                outdir=str(extract_dir),
                verbosity=-1,  # Suppress patool output
            )
            logger.debug(f"Extracted {archive.name} to {extract_dir}")

            # Process extracted files
            for item in extract_dir.rglob("*"):
                if not item.is_file():
                    continue

                # Apply filter first (don't delete files, just skip them)
                if should_extract and not should_extract(item):
                    logger.debug(f"Skipping {item.name} based on filter")
                    continue

                # If the file passed the filter, process it
                extracted_files.append(item)
                logger.debug(f"Kept {item.relative_to(target_dir)}")

                if extract_callback:
                    extract_callback(item)

                # If it's an archive, recursively extract it
                if _is_archive(item) and current_depth < max_depth - 1:
                    logger.info(f"Found nested archive: {item.name}")
                    nested_files = _extract_recursive_nested(item, item.parent, current_depth + 1)
                    extracted_files.extend(nested_files)

        except Exception as e:
            logger.error(f"Failed to extract archive {archive.name}: {e}")

        return extracted_files

    # Dispatch to the appropriate internal helper
    if flatten:
        return _extract_recursive_flat(archive_path, 0)
    else:
        return _extract_recursive_nested(archive_path, output_dir, 0)


def _is_archive(file_path: Path) -> bool:
    """Check if a file is an archive that can be extracted."""
    try:
        # patoolib can check if a file is a supported archive
        patoolib.get_archive_format(str(file_path))
        return True
    except Exception:
        return False


def _get_unique_path(directory: Path, original_path: Path) -> Path:
    """Generate a unique path in a directory to avoid overwriting files."""
    final_path = directory / original_path.name
    if not final_path.exists():
        return final_path

    stem = original_path.stem
    suffix = original_path.suffix
    counter = 1
    while final_path.exists():
        final_path = directory / f"{stem}_{counter}{suffix}"
        counter += 1
    logger.debug(f"Renamed {original_path.name} to {final_path.name} to avoid conflict")
    return final_path


def _move_file_to_destination(
    source: Path,
    dest_dir: Path,
    should_extract: Callable[[Path], bool] | None,
    extract_callback: Callable[[Path], None] | None,
) -> Path | None:
    """
    Move a file to the destination directory, applying filters and callbacks.

    Returns:
        The destination path if successful, None if filtered out or failed
    """
    # Apply filter if provided
    if should_extract and not should_extract(source):
        logger.debug(f"Skipping {source.name} based on filter")
        return None

    # Get unique destination path
    dest_path = _get_unique_path(dest_dir, source)

    try:
        shutil.move(str(source), str(dest_path))
        logger.debug(f"Moved {source.name} to final destination")

        if extract_callback:
            extract_callback(dest_path)

        return dest_path
    except Exception as e:
        logger.error(f"Failed to move {source.name} to destination: {e}")
        return None
