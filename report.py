import os
from pathlib import Path

# ---------------- CONFIG ----------------
project_root = Path(__file__).parent  # or absolute path if needed

# Folders to hide files inside (show only folder names)
hide_files_in = {
    project_root / "data" / "raw" / "ready_made",
    project_root / "data" / "raw" / "kaggle",
    project_root / "data" / "processed",
    project_root / "data" / "deduplicated",
}

# Folders to hide completely (skip everything inside)
hide_folders_completely = {
    project_root / "venv",
    project_root / ".kaggle",
    project_root / ".kaggle_preview",
    project_root / "logs",  # optional, hide logs completely
}

# Output file
report_file = project_root / "project_report.txt"

# ---------------- FUNCTION ----------------
def should_hide_files(dirpath_obj):
    return any(dirpath_obj == hide_dir or hide_dir in dirpath_obj.parents for hide_dir in hide_files_in)

def should_skip_dir(dirpath_obj):
    return any(dirpath_obj == skip_dir or skip_dir in dirpath_obj.parents for skip_dir in hide_folders_completely)

# ---------------- GENERATE REPORT ----------------
with report_file.open("w", encoding="utf-8") as f:
    f.write(f"Project structure report for {project_root}\n")
    f.write("="*80 + "\n\n")

    for dirpath, dirnames, filenames in os.walk(project_root):
        dirpath_obj = Path(dirpath)

        # Skip completely irrelevant directories
        if should_skip_dir(dirpath_obj):
            continue

        # Compute indentation based on depth
        level = len(dirpath_obj.relative_to(project_root).parts)
        indent = " " * 4 * level

        # Write folder name
        f.write(f"{indent}{dirpath_obj.name}/\n")

        # Write files if not in hidden-files folders
        if not should_hide_files(dirpath_obj):
            subindent = " " * 4 * (level + 1)
            for filename in filenames:
                f.write(f"{subindent}{filename}\n")

print(f"✅ Project report generated at: {report_file}")
