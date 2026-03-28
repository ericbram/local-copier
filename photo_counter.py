#!/usr/bin/env python3
"""
Enumerate photos in a library folder and count them by year.

Usage:
    python3 photo_counter.py [path_to_photos_folder]

If no path is given, defaults to ~/Pictures/Photos Library.photoslibrary/originals

NOTE: To access the macOS Photos Library, Terminal (or your IDE) needs
Full Disk Access: System Settings > Privacy & Security > Full Disk Access
"""

import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

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

DEFAULT_LIBRARY = os.path.expanduser(
    "~/Pictures/Photos Library.photoslibrary/originals"
)


def count_photos_by_year(library_path: str) -> dict[int, int]:
    """Walk the library and count media files by year (file modification date)."""
    counts: dict[int, int] = defaultdict(int)
    skipped = 0

    for root, _dirs, files in os.walk(library_path):
        for filename in files:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in ALL_MEDIA_EXTENSIONS:
                continue

            filepath = os.path.join(root, filename)
            try:
                mtime = os.path.getmtime(filepath)
                year = datetime.fromtimestamp(mtime).year
                counts[year] += 1
            except OSError:
                skipped += 1

    if skipped:
        print(f"  (skipped {skipped} files due to read errors)")

    return dict(counts)


def move_photos_by_year(library_path: str, destination: str, counts: dict[int, int]):
    """
    TODO: Move photos into per-year folders at the destination.

    Will create structure like:
        destination/2023/photo1.jpg
        destination/2023/photo2.heic
        destination/2024/photo3.jpg
        ...
    """
    print(f"\n[STUB] Would move {sum(counts.values())} files from:")
    print(f"  Source:      {library_path}")
    print(f"  Destination: {destination}")
    for year in sorted(counts):
        print(f"    {destination}/{year}/  ← {counts[year]} files")


def main():
    if len(sys.argv) > 1:
        library_path = sys.argv[1]
    else:
        library_path = DEFAULT_LIBRARY

    library_path = os.path.expanduser(library_path)

    if not os.path.isdir(library_path):
        print(f"Error: '{library_path}' is not a directory or is not accessible.")
        print()
        print("If using the macOS Photos Library, grant Full Disk Access to Terminal:")
        print("  System Settings > Privacy & Security > Full Disk Access > Terminal")
        sys.exit(1)

    print(f"Scanning: {library_path}\n")

    counts = count_photos_by_year(library_path)

    if not counts:
        print("No media files found.")
        sys.exit(0)

    print("Photos/videos by year:")
    print("-" * 30)
    for year in sorted(counts):
        print(f"  {year}:  {counts[year]:,}")
    print("-" * 30)
    print(f"  Total: {sum(counts.values()):,}")

    # Stub: uncomment and set destination when ready to move files
    # move_photos_by_year(library_path, "/path/to/destination", counts)


if __name__ == "__main__":
    main()
