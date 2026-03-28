#!/usr/bin/env python3
"""
Enumerate photos in a macOS Photos Library, count by year, and copy to a destination
using original filenames (not GUIDs).

Usage:
    # Count only (dry run, no copying)
    python3 library_copy.py --dry-run

    # Copy photos to destination organized by year
    python3 library_copy.py

    # Custom source and destination
    python3 library_copy.py --source ~/Pictures/Photos\ Library.photoslibrary --destination /path/to/dest

NOTE: To access the macOS Photos Library, Terminal (or your IDE) needs
Full Disk Access: System Settings > Privacy & Security > Full Disk Access
"""

import argparse
import hashlib
import os
import shutil
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Common photo/video extensions
PHOTO_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".heic", ".heif", ".tiff", ".tif",
    ".bmp", ".gif", ".webp", ".raw", ".cr2", ".cr3", ".nef",
    ".arw", ".dng", ".orf", ".rw2", ".pef", ".sr2", ".raf",
}
VIDEO_EXTENSIONS = {
    ".mov", ".mp4", ".m4v", ".avi", ".mkv", ".3gp",
}
ALL_MEDIA_EXTENSIONS = PHOTO_EXTENSIONS | VIDEO_EXTENSIONS

DEFAULT_SOURCE = os.path.expanduser(
    "~/Pictures/Photos Library.photoslibrary"
)
DEFAULT_DESTINATION = "/Volumes/Karolina/FINAL PHOTOS"

# Apple's Core Data epoch: 2001-01-01 00:00:00 UTC
APPLE_EPOCH_OFFSET = 978307200


def file_checksum(filepath):
    # type: (str) -> str
    """Compute MD5 checksum of a file."""
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def load_filename_map(library_path):
    # type: (str) -> Dict[str, Tuple[str, Optional[int]]]
    """
    Query the Photos SQLite database to build a map of:
        GUID filename (without extension) -> (original filename, year from EXIF/photo date)

    The database stores the directory (GUID) and original filename for each asset.
    """
    db_path = os.path.join(library_path, "database", "Photos.sqlite")
    if not os.path.exists(db_path):
        print(f"Warning: Photos database not found at {db_path}")
        print("         Will use GUID filenames as-is.")
        return {}

    filename_map = {}
    try:
        # Connect read-only to avoid any accidental modifications
        conn = sqlite3.connect("file:" + db_path + "?mode=ro", uri=True)
        cursor = conn.cursor()

        # ZASSET table contains: ZDIRECTORY (subfolder under originals/),
        # ZORIGINALFILENAME (the real name), and ZDATECREATED (photo date)
        cursor.execute("""
            SELECT ZDIRECTORY, ZFILENAME, ZORIGINALFILENAME, ZDATECREATED
            FROM ZASSET
            WHERE ZDIRECTORY IS NOT NULL
              AND ZFILENAME IS NOT NULL
              AND ZORIGINALFILENAME IS NOT NULL
        """)

        for directory, guid_filename, original_filename, date_created in cursor.fetchall():
            # Key: the GUID filename as stored on disk (e.g., "IMG_1234.HEIC" or a GUID)
            # We key by directory/guid_filename to handle uniqueness
            key = os.path.join(directory, guid_filename)

            # Convert Apple Core Data timestamp to year
            year = None
            if date_created is not None:
                try:
                    timestamp = date_created + APPLE_EPOCH_OFFSET
                    year = datetime.fromtimestamp(timestamp).year
                except (OSError, ValueError, OverflowError):
                    pass

            filename_map[key] = (original_filename, year)

        conn.close()
        print(f"  Loaded {len(filename_map):,} filename mappings from Photos database")
    except sqlite3.Error as e:
        print(f"Warning: Could not read Photos database: {e}")
        print("         Will use GUID filenames as-is.")

    return filename_map


def find_media_files(library_path, filename_map):
    # type: (str, Dict[str, Tuple[str, Optional[int]]]) -> List[Tuple[str, str, int]]
    """
    Walk the library originals folder and return list of:
        (source_filepath, destination_filename, year)

    Uses the filename_map to resolve GUIDs to original names and get accurate years.
    Falls back to the on-disk filename and file modification time if not in the map.
    """
    originals_path = os.path.join(library_path, "originals")
    if not os.path.isdir(originals_path):
        print(f"Error: originals folder not found at {originals_path}")
        sys.exit(1)

    files_found = []
    matched = 0
    unmatched = 0

    for root, _dirs, files in os.walk(originals_path):
        for filename in files:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in ALL_MEDIA_EXTENSIONS:
                continue

            filepath = os.path.join(root, filename)

            # Build the relative key: e.g., "A/ABC12345-1234-..../IMG_1234.HEIC"
            rel_path = os.path.relpath(filepath, originals_path)
            # The database stores directory as the subfolder (e.g., "A/ABC12345...")
            # and filename separately, so the key is directory/filename
            lookup_key = rel_path

            if lookup_key in filename_map:
                original_name, db_year = filename_map[lookup_key]
                matched += 1

                # Use year from database if available, otherwise fall back to mtime
                if db_year is not None:
                    year = db_year
                else:
                    try:
                        mtime = os.path.getmtime(filepath)
                        year = datetime.fromtimestamp(mtime).year
                    except OSError:
                        continue
            else:
                # Not in database — use filename and mtime as-is
                original_name = filename
                unmatched += 1
                try:
                    mtime = os.path.getmtime(filepath)
                    year = datetime.fromtimestamp(mtime).year
                except OSError:
                    continue

            files_found.append((filepath, original_name, year))

    print(f"  Matched to original names: {matched:,}")
    if unmatched:
        print(f"  Using GUID names (no DB match): {unmatched:,}")

    return files_found


def print_counts(media_files):
    # type: (List[Tuple[str, str, int]]) -> None
    """Print photo/video counts bucketed by year."""
    counts = defaultdict(int)
    for _, _, year in media_files:
        counts[year] += 1

    print("Photos/videos by year:")
    print("-" * 30)
    for year in sorted(counts):
        print(f"  {year}:  {counts[year]:,}")
    print("-" * 30)
    print(f"  Total: {sum(counts.values()):,}")


def resolve_destination_path(dest_dir, filename, src_checksum):
    # type: (str, str, str) -> Optional[str]
    """
    Determine the final destination path for a file.

    Returns:
        - None if the file already exists with matching checksum (skip)
        - The path to copy to (possibly with a counter suffix if name conflicts)
    """
    dest_path = os.path.join(dest_dir, filename)

    if not os.path.exists(dest_path):
        return dest_path

    # File exists — check checksum
    if file_checksum(dest_path) == src_checksum:
        return None  # Identical file, skip

    # Same name but different content — add counter suffix
    name, ext = os.path.splitext(filename)
    counter = 1
    while True:
        new_filename = "{}_{}{}".format(name, counter, ext)
        new_dest_path = os.path.join(dest_dir, new_filename)
        if not os.path.exists(new_dest_path):
            return new_dest_path
        if file_checksum(new_dest_path) == src_checksum:
            return None  # Already copied with a counter suffix
        counter += 1


def copy_photos_by_year(media_files, destination, dry_run, test=False):
    # type: (List[Tuple[str, str, int]], str, bool, bool) -> None
    """Copy media files into per-year folders at the destination."""
    copied = 0
    skipped_identical = 0
    skipped_error = 0
    renamed = 0

    total = len(media_files)

    for i, (src_path, dest_filename, year) in enumerate(media_files, 1):
        dest_dir = os.path.join(destination, str(year))

        if dry_run:
            print("  [DRY RUN] {} -> {}/{}".format(src_path, dest_dir, dest_filename))
            copied += 1
            continue

        try:
            src_checksum = file_checksum(src_path)
        except OSError as e:
            print("  [ERROR] Cannot read {}: {}".format(src_path, e))
            skipped_error += 1
            continue

        os.makedirs(dest_dir, exist_ok=True)

        final_path = resolve_destination_path(dest_dir, dest_filename, src_checksum)

        if final_path is None:
            skipped_identical += 1
            continue

        final_filename = os.path.basename(final_path)
        if final_filename != dest_filename:
            renamed += 1

        try:
            shutil.copy2(src_path, final_path)
            # Verify copy integrity
            dest_checksum = file_checksum(final_path)
            if src_checksum != dest_checksum:
                print("  [ERROR] Checksum mismatch after copy: {}".format(src_path))
                print("          Source: {}  Dest: {}".format(src_checksum, dest_checksum))
                os.remove(final_path)
                skipped_error += 1
                continue
            copied += 1
            if test:
                print("  [TEST] Successfully copied 1 file: {} -> {}".format(src_path, final_path))
                break
            if i % 100 == 0 or i == total:
                print("  Progress: {:,}/{:,} files processed...".format(i, total))
        except OSError as e:
            print("  [ERROR] Failed to copy {}: {}".format(src_path, e))
            skipped_error += 1

    print()
    print("=" * 40)
    if dry_run:
        print("DRY RUN complete: {:,} files would be copied".format(copied))
        print("Total files found: {:,}".format(total))
    else:
        print("Copied:              {:,}".format(copied))
        print("Renamed (dupes):     {:,}".format(renamed))
        print("Skipped (identical): {:,}".format(skipped_identical))
        if skipped_error:
            print("Skipped (errors):    {:,}".format(skipped_error))
        print("Total files found:   {:,}".format(total))


def main():
    parser = argparse.ArgumentParser(
        description="Count and copy photos from a macOS Photos Library into per-year folders, using original filenames."
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help="Source Photos Library path (default: {})".format(DEFAULT_SOURCE),
    )
    parser.add_argument(
        "--destination",
        default=DEFAULT_DESTINATION,
        help="Destination root folder (default: {})".format(DEFAULT_DESTINATION),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be copied, don't actually copy",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Copy only 1 file to verify everything works, then stop",
    )
    args = parser.parse_args()

    source = os.path.expanduser(args.source)
    destination = os.path.expanduser(args.destination)

    if not os.path.isdir(source):
        print("Error: Source '{}' is not a directory or is not accessible.".format(source))
        print()
        print("If using the macOS Photos Library, grant Full Disk Access to Terminal:")
        print("  System Settings > Privacy & Security > Full Disk Access > Terminal")
        sys.exit(1)

    print("Source:      {}".format(source))
    print("Destination: {}".format(destination))
    mode = "TEST (1 file)" if args.test else ("DRY RUN" if args.dry_run else "COPY")
    print("Mode:        {}".format(mode))
    print()

    print("Loading Photos database...")
    filename_map = load_filename_map(source)
    print()

    print("Scanning source...")
    media_files = find_media_files(source, filename_map)
    print()

    print_counts(media_files)
    print()

    if not args.dry_run and not args.test and not os.path.isdir(destination):
        print("Error: Destination '{}' is not accessible.".format(destination))
        print("Make sure the NAS is mounted (Finder > Go > Connect to Server).")
        sys.exit(1)

    if args.test and not os.path.isdir(destination):
        print("Error: Destination '{}' is not accessible.".format(destination))
        print("Make sure the NAS is mounted (Finder > Go > Connect to Server).")
        sys.exit(1)

    copy_photos_by_year(media_files, destination, args.dry_run, args.test)


if __name__ == "__main__":
    main()
