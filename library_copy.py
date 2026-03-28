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
import stat
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


def discover_db_schema(library_path):
    # type: (str) -> None
    """Print all tables and columns in the Photos database for debugging."""
    db_path = os.path.join(library_path, "database", "Photos.sqlite")
    if not os.path.exists(db_path):
        print("  Photos database not found at {}".format(db_path))
        return

    try:
        conn = sqlite3.connect("file:" + db_path + "?mode=ro", uri=True)
        cursor = conn.cursor()

        # Get ZASSET columns
        cursor.execute("PRAGMA table_info(ZASSET)")
        columns = cursor.fetchall()
        print("  ZASSET table has {} columns".format(len(columns)))

        # Print columns that look relevant to filenames/names/dates
        print("  Filename-related columns:")
        for col in columns:
            col_name = col[1]
            if any(kw in col_name.upper() for kw in [
                "FILE", "NAME", "ORIGINAL", "TITLE", "DIR", "PATH",
                "DATE", "IMPORT", "UUID"
            ]):
                print("    {} (type: {})".format(col_name, col[2]))

        # Also check for other asset-related tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [r[0] for r in cursor.fetchall()]
        asset_tables = [t for t in tables if "ASSET" in t.upper() or "ADDITIONAL" in t.upper()]
        if asset_tables:
            print("  Other asset-related tables: {}".format(", ".join(asset_tables)))
            for t in asset_tables:
                if t != "ZASSET":
                    cursor.execute("PRAGMA table_info({})".format(t))
                    t_cols = cursor.fetchall()
                    name_cols = [c[1] for c in t_cols if any(
                        kw in c[1].upper() for kw in ["FILE", "NAME", "ORIGINAL", "TITLE"]
                    )]
                    if name_cols:
                        print("    {}: {}".format(t, ", ".join(name_cols)))

        conn.close()
    except sqlite3.Error as e:
        print("  [DB error during schema discovery: {}]".format(e))


def load_filename_map(library_path):
    # type: (str) -> Dict[str, Tuple[str, Optional[int]]]
    """
    Query the Photos SQLite database to build a map of:
        disk path (relative to originals/) -> (original filename, year from photo date)

    On macOS Sequoia (15.x), original filenames are in ZADDITIONALASSETATTRIBUTES
    joined to ZASSET. On older versions they may be directly in ZASSET.
    """
    db_path = os.path.join(library_path, "database", "Photos.sqlite")
    if not os.path.exists(db_path):
        print("Warning: Photos database not found at {}".format(db_path))
        print("         Will use GUID filenames as-is.")
        return {}

    filename_map = {}
    try:
        conn = sqlite3.connect("file:" + db_path + "?mode=ro", uri=True)
        cursor = conn.cursor()

        # Check which tables have the original filename
        cursor.execute("PRAGMA table_info(ZASSET)")
        asset_columns = {col[1] for col in cursor.fetchall()}

        cursor.execute("PRAGMA table_info(ZADDITIONALASSETATTRIBUTES)")
        additional_columns = {col[1] for col in cursor.fetchall()}

        # Determine where ZORIGINALFILENAME lives
        original_in_additional = "ZORIGINALFILENAME" in additional_columns
        original_in_asset = "ZORIGINALFILENAME" in asset_columns

        # Find the best date column in ZASSET
        date_col = None
        for candidate in ["ZDATECREATED", "ZADDEDDATE", "ZMODIFICATIONDATE"]:
            if candidate in asset_columns:
                date_col = candidate
                break

        if original_in_additional:
            # Sequoia (15.x) and newer: join ZASSET with ZADDITIONALASSETATTRIBUTES
            print("  Schema: ZORIGINALFILENAME in ZADDITIONALASSETATTRIBUTES (Sequoia+)")
            query = """
                SELECT A.ZDIRECTORY, A.ZFILENAME, B.ZORIGINALFILENAME{}
                FROM ZASSET A
                INNER JOIN ZADDITIONALASSETATTRIBUTES B ON B.ZASSET = A.Z_PK
                WHERE A.ZDIRECTORY IS NOT NULL
                  AND A.ZFILENAME IS NOT NULL
                  AND B.ZORIGINALFILENAME IS NOT NULL
            """.format(", A.{}".format(date_col) if date_col else "")
        elif original_in_asset:
            # Older macOS: original filename directly in ZASSET
            print("  Schema: ZORIGINALFILENAME in ZASSET (pre-Sequoia)")
            query = """
                SELECT ZDIRECTORY, ZFILENAME, ZORIGINALFILENAME{}
                FROM ZASSET
                WHERE ZDIRECTORY IS NOT NULL
                  AND ZFILENAME IS NOT NULL
                  AND ZORIGINALFILENAME IS NOT NULL
            """.format(", {}".format(date_col) if date_col else "")
        else:
            print("Warning: ZORIGINALFILENAME not found in either ZASSET or ZADDITIONALASSETATTRIBUTES")
            discover_db_schema(library_path)
            conn.close()
            return {}

        print("  Date column: {}".format(date_col or "NONE (using file mtime)"))

        cursor.execute(query)

        for row in cursor.fetchall():
            directory = row[0]
            guid_filename = row[1]
            original_filename = row[2]
            date_created = row[3] if date_col else None

            key = os.path.join(directory, guid_filename)

            year = None
            if date_created is not None:
                try:
                    timestamp = date_created + APPLE_EPOCH_OFFSET
                    year = datetime.fromtimestamp(timestamp).year
                except (OSError, ValueError, OverflowError):
                    pass

            filename_map[key] = (original_filename, year)

        conn.close()
        print("  Loaded {:,} filename mappings from Photos database".format(len(filename_map)))
    except sqlite3.Error as e:
        print("Warning: Could not read Photos database: {}".format(e))
        print("         Will use GUID filenames as-is.")
        discover_db_schema(library_path)

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
    skipped_no_name = []
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

                files_found.append((filepath, original_name, year))
            else:
                # Not in database — skip (no original name available)
                skipped_no_name.append(filepath)
                unmatched += 1

    print("  Matched to original names: {:,}".format(matched))
    if unmatched:
        print("  Skipped (no original name): {:,}".format(unmatched))
        # Print first few skipped files for reference
        for path in skipped_no_name[:5]:
            print("    e.g. {}".format(os.path.basename(path)))
        if len(skipped_no_name) > 5:
            print("    ... and {:,} more".format(len(skipped_no_name) - 5))

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


def print_test_diagnostics(src_path, dest_filename, year, library_path):
    # type: (str, str, int, str) -> None
    """Print detailed diagnostics about a file for --test mode."""
    print()
    print("=" * 60)
    print("FILE DIAGNOSTICS")
    print("=" * 60)

    # Filesystem info
    print("\n--- Filesystem ---")
    print("  Source path:      {}".format(src_path))
    print("  On-disk filename: {}".format(os.path.basename(src_path)))
    print("  Resolved name:    {}".format(dest_filename))
    print("  Assigned year:    {}".format(year))
    try:
        st = os.stat(src_path)
        print("  File size:        {:,} bytes".format(st.st_size))
        print("  Created (ctime):  {}".format(datetime.fromtimestamp(st.st_ctime)))
        print("  Modified (mtime): {}".format(datetime.fromtimestamp(st.st_mtime)))
        print("  Accessed (atime): {}".format(datetime.fromtimestamp(st.st_atime)))
        if hasattr(st, "st_birthtime"):
            print("  Birth time:       {}".format(datetime.fromtimestamp(st.st_birthtime)))
    except OSError as e:
        print("  [ERROR reading stat: {}]".format(e))

    # Database lookup
    originals_path = os.path.join(library_path, "originals")
    rel_path = os.path.relpath(src_path, originals_path)
    parts = rel_path.split(os.sep)
    # directory is everything except the last component (filename)
    if len(parts) >= 2:
        directory = os.path.join(*parts[:-1])
        disk_filename = parts[-1]
    else:
        directory = ""
        disk_filename = rel_path

    print("\n--- Database Lookup ---")
    print("  Relative path:  {}".format(rel_path))
    print("  Directory key:   {}".format(directory))
    print("  Filename key:    {}".format(disk_filename))

    db_path = os.path.join(library_path, "database", "Photos.sqlite")
    if not os.path.exists(db_path):
        print("  [Photos database not found]")
        return

    try:
        conn = sqlite3.connect("file:" + db_path + "?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get all columns for this asset
        cursor.execute("""
            SELECT * FROM ZASSET
            WHERE ZDIRECTORY = ? AND ZFILENAME = ?
        """, (directory, disk_filename))
        row = cursor.fetchone()

        if row:
            print("  [MATCH FOUND in ZASSET]")
            print()
            print("  --- All ZASSET columns for this file ---")
            for key in row.keys():
                val = row[key]
                if val is not None:
                    # Convert Apple timestamps for readability
                    if "DATE" in key.upper() and isinstance(val, (int, float)):
                        try:
                            ts = val + APPLE_EPOCH_OFFSET
                            human = datetime.fromtimestamp(ts)
                            print("    {}: {} ({})".format(key, val, human))
                        except (OSError, ValueError, OverflowError):
                            print("    {}: {}".format(key, val))
                    else:
                        print("    {}: {}".format(key, val))
        else:
            print("  [NO MATCH in ZASSET for directory='{}', filename='{}']".format(
                directory, disk_filename))

            # Try a broader search by just filename
            cursor.execute("""
                SELECT * FROM ZASSET WHERE ZFILENAME = ?
            """, (disk_filename,))
            broader = cursor.fetchall()
            if broader:
                print("  [Broader search by filename only found {} match(es)]".format(len(broader)))
                for r in broader:
                    print("  --- Matched row (all non-null columns) ---")
                    for key in r.keys():
                        val = r[key]
                        if val is not None:
                            if "DATE" in key.upper() and isinstance(val, (int, float)):
                                try:
                                    ts = val + APPLE_EPOCH_OFFSET
                                    human = datetime.fromtimestamp(ts)
                                    print("    {}: {} ({})".format(key, val, human))
                                except (OSError, ValueError, OverflowError):
                                    print("    {}: {}".format(key, val))
                            else:
                                print("    {}: {}".format(key, val))
            else:
                print("  [No match even by filename alone]")

                # Try searching by directory
                cursor.execute("""
                    SELECT * FROM ZASSET WHERE ZDIRECTORY = ?
                """, (directory,))
                dir_matches = cursor.fetchall()
                if dir_matches:
                    print("  [Files in same directory in DB ({} matches):]".format(len(dir_matches)))
                    for r in dir_matches[:3]:  # Show first 3
                        print("  --- Row ---")
                        for key in r.keys():
                            val = r[key]
                            if val is not None:
                                if "DATE" in key.upper() and isinstance(val, (int, float)):
                                    try:
                                        ts = val + APPLE_EPOCH_OFFSET
                                        human = datetime.fromtimestamp(ts)
                                        print("    {}: {} ({})".format(key, val, human))
                                    except (OSError, ValueError, OverflowError):
                                        print("    {}: {}".format(key, val))
                                else:
                                    print("    {}: {}".format(key, val))

        conn.close()
    except sqlite3.Error as e:
        print("  [DB error: {}]".format(e))

    print("=" * 60)
    print()


def copy_photos_by_year(media_files, destination, dry_run, test=False, library_path=None):
    # type: (List[Tuple[str, str, int]], str, bool, bool) -> None
    """Copy media files into per-year folders at the destination, then delete source."""
    copied = 0
    deleted = 0
    skipped_identical = 0
    skipped_error = 0
    renamed = 0

    total = len(media_files)
    last_percent_printed = -1

    print()
    if dry_run:
        print("Analyzing {:,} files...".format(total))
    elif test:
        print("Running test copy (1 file)...")
    else:
        print("Copying {:,} files...".format(total))
    print()

    for i, (src_path, dest_filename, year) in enumerate(media_files, 1):
        dest_dir = os.path.join(destination, str(year))

        # Print percentage progress
        if total > 0:
            pct = (i * 100) // total
            if pct > last_percent_printed:
                last_percent_printed = pct
                print("{}%".format(pct))

        if dry_run:
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
            # Delete source after verified copy (not in test mode)
            if not test:
                try:
                    os.remove(src_path)
                    deleted += 1
                except OSError as e:
                    print("  [WARNING] Copied OK but failed to delete source: {}".format(e))
            if test:
                print("  [TEST] Successfully copied 1 file: {} -> {}".format(src_path, final_path))
                if library_path:
                    print_test_diagnostics(src_path, dest_filename, year, library_path)
                break
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
        print("Source deleted:      {:,}".format(deleted))
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

    copy_photos_by_year(media_files, destination, args.dry_run, args.test, source)


if __name__ == "__main__":
    main()
