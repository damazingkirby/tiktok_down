import os
import re

def rename_tiktok_videos(downloads_dir="downloads"):
    """
    Renames TikTok videos from the format 'title [id].ext' to '[id] title.ext'.
    """
    if not os.path.exists(downloads_dir):
        print(f"Error: The directory '{downloads_dir}' does not exist.")
        return

    # Regex to match: any characters, optionally ending with a space, then [id], then .extension
    # Examples it matches:
    # "My Video [12345].mp4" -> group 1: "My Video", group 2: "12345", group 3: ".mp4"
    # "#fyp [67890].mp4" -> group 1: "#fyp", group 2: "67890", group 3: ".mp4"
    # "[999] [111].mp4" -> group 1: "[999]", group 2: "111", group 3: ".mp4"
    # Note: TikTok IDs are typically 18-19 digits long, so we ensure the ID is just digits.
    pattern = re.compile(r"^(.*?) ?\[(\d+)\](\.[a-zA-Z0-9_]+)$")

    renamed_count = 0
    skipped_count = 0
    error_count = 0

    print(f"Scanning directory: {downloads_dir}\n")

    for root, dirs, files in os.walk(downloads_dir):
        for filename in files:
            # Skip archive.txt or any other known non-media files
            if filename == "archive.txt":
                continue
            
            filepath = os.path.join(root, filename)
            
            # Check if the file is already in the target format: [id] title.ext
            # ^\[\d+\].*
            if re.match(r"^\[\d+\].*", filename):
                skipped_count += 1
                continue

            match = pattern.match(filename)
            if match:
                title = match.group(1).strip()
                video_id = match.group(2)
                extension = match.group(3)

                # Construct new filename
                # If there's no title (e.g. " [123].mp4"), just don't add a space
                if title:
                    new_filename = f"[{video_id}] {title}{extension}"
                else:
                    new_filename = f"[{video_id}]{extension}"
                
                new_filepath = os.path.join(root, new_filename)

                # Make sure we don't overwrite an existing file accidentally
                if os.path.exists(new_filepath):
                    print(f"Skipping {filename}: target name {new_filename} already exists.")
                    skipped_count += 1
                    continue

                try:
                    os.rename(filepath, new_filepath)
                    print(f"Renamed: '{filename}' -> '{new_filename}'")
                    renamed_count += 1
                except Exception as e:
                    print(f"Error renaming '{filename}': {e}")
                    error_count += 1
            else:
                # File didn't match the expected pattern
                skipped_count += 1

    print("\n--- Summary ---")
    print(f"Successfully renamed: {renamed_count} files")
    print(f"Skipped (already correct or unrecognized format): {skipped_count} files")
    print(f"Errors: {error_count} files")

if __name__ == "__main__":
    rename_tiktok_videos()
