#!/usr/bin/env python3
"""
Enumerate photos in a library folder, count by year, and copy to a destination.

Usage:
    # Count only (dry run, no copying)
    python3 photo_counter.py --dry-run

    # Copy photos to destination organized by year
    python3 photo_counter.py

    # Custom source and destination
    python3 photo_counter.py --source /path/to/photos --destination /path/to/dest

NOTE: To access the macOS Photos Library, Terminal (or your IDE) needs
Full Disk Access: System Settings > Privacy & Security > Full Disk Access
"""

import argparse
import hashlib
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime

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
    "~/Pictures/Photos Library.photoslibrary/originals"
)
DEFAULT_DESTINATION = "/Volumes/Karolina/FINAL PHOTOS"


def file_checksum(filepath: str) -> str:
    """Compute MD5 checksum of a file."""
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def find_media_files(library_path: str) -> list[tuple[str, int]]:
    """Walk the library and return list of (filepath, year) for all media files."""
    files_found = []
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
                files_found.append((filepath, year))
            except OSError:
                skipped += 1

    if skipped:
        print(f"  (skipped {skipped} files due to read errors)")

    return files_found


def print_counts(media_files: list[tuple[str, int]]):
    """Print photo/video counts bucketed by year."""
    counts: dict[int, int] = defaultdict(int)
    for _, year in media_files:
        counts[year] += 1

    print("Photos/videos by year:")
    print("-" * 30)
    for year in sorted(counts):
        print(f"  {year}:  {counts[year]:,}")
    print("-" * 30)
    print(f"  Total: {sum(counts.values()):,}")


def resolve_destination_path(dest_dir: str, filename: str, src_checksum: str) -> str | None:
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
        new_filename = f"{name}_{counter}{ext}"
        new_dest_path = os.path.join(dest_dir, new_filename)
        if not os.path.exists(new_dest_path):
            return new_dest_path
        if file_checksum(new_dest_path) == src_checksum:
            return None  # Already copied with a counter suffix
        counter += 1


def copy_photos_by_year(media_files: list[tuple[str, int]], destination: str, dry_run: bool, test: bool = False):
    """Copy media files into per-year folders at the destination."""
    copied = 0
    skipped_identical = 0
    skipped_error = 0
    renamed = 0

    total = len(media_files)

    for i, (src_path, year) in enumerate(media_files, 1):
        filename = os.path.basename(src_path)
        dest_dir = os.path.join(destination, str(year))

        if dry_run:
            print(f"  [DRY RUN] {src_path} -> {dest_dir}/{filename}")
            copied += 1
            continue

        try:
            src_checksum = file_checksum(src_path)
        except OSError as e:
            print(f"  [ERROR] Cannot read {src_path}: {e}")
            skipped_error += 1
            continue

        os.makedirs(dest_dir, exist_ok=True)

        final_path = resolve_destination_path(dest_dir, filename, src_checksum)

        if final_path is None:
            skipped_identical += 1
            continue

        final_filename = os.path.basename(final_path)
        if final_filename != filename:
            renamed += 1

        try:
            shutil.copy2(src_path, final_path)
            # Verify copy integrity
            dest_checksum = file_checksum(final_path)
            if src_checksum != dest_checksum:
                print(f"  [ERROR] Checksum mismatch after copy: {src_path}")
                print(f"          Source: {src_checksum}  Dest: {dest_checksum}")
                os.remove(final_path)
                skipped_error += 1
                continue
            copied += 1
            if test:
                print(f"  [TEST] Successfully copied 1 file: {src_path} -> {final_path}")
                break
            if i % 100 == 0 or i == total:
                print(f"  Progress: {i:,}/{total:,} files processed...")
        except OSError as e:
            print(f"  [ERROR] Failed to copy {src_path}: {e}")
            skipped_error += 1

    print()
    print("=" * 40)
    if dry_run:
        print(f"DRY RUN complete: {copied:,} files would be copied")
        print(f"Total files found: {total:,}")
    else:
        print(f"Copied:              {copied:,}")
        print(f"Renamed (dupes):     {renamed:,}")
        print(f"Skipped (identical): {skipped_identical:,}")
        if skipped_error:
            print(f"Skipped (errors):    {skipped_error:,}")
        print(f"Total files found:   {total:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Count and copy photos from a library into per-year folders."
    )
    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help=f"Source photo library path (default: {DEFAULT_SOURCE})",
    )
    parser.add_argument(
        "--destination",
        default=DEFAULT_DESTINATION,
        help=f"Destination root folder (default: {DEFAULT_DESTINATION})",
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
        print(f"Error: Source '{source}' is not a directory or is not accessible.")
        print()
        print("If using the macOS Photos Library, grant Full Disk Access to Terminal:")
        print("  System Settings > Privacy & Security > Full Disk Access > Terminal")
        sys.exit(1)

    print(f"Source:      {source}")
    print(f"Destination: {destination}")
    mode = "TEST (1 file)" if args.test else ("DRY RUN" if args.dry_run else "COPY")
    print(f"Mode:        {mode}")
    print()

    print("Scanning source...")
    media_files = find_media_files(source)

    if not media_files:
        print("No media files found.")
        sys.exit(0)

    print_counts(media_files)
    print()

    if not args.dry_run and not args.test and not os.path.isdir(destination):
        print(f"Error: Destination '{destination}' is not accessible.")
        print("Make sure the NAS is mounted (Finder > Go > Connect to Server).")
        sys.exit(1)

    if args.test and not os.path.isdir(destination):
        print(f"Error: Destination '{destination}' is not accessible.")
        print("Make sure the NAS is mounted (Finder > Go > Connect to Server).")
        sys.exit(1)

    copy_photos_by_year(media_files, destination, args.dry_run, args.test)


if __name__ == "__main__":
    main()
