
import os
from pathlib import Path

# Change this to your project directory.
PROJECT_DIR = Path(".").resolve()

# Output file will be created outside the project directory
# to prevent it from being included in the context.
OUTPUT_FILE = PROJECT_DIR.parent / "project_context.txt"

IGNORED_DIRS = {
    ".git",
    ".vscode",
    ".idea",
    ".vs",
    "__pycache__",
    "node_modules",
    "venv",
    ".venv",
    "env",
    "dist",
    "build",
    "out",
    "target",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".next",
    ".nuxt",
    "coverage",
    "files"
}

IGNORED_FILES = {
    ".DS_Store",
    "Thumbs.db",
    "project_context.txt",
}

IGNORED_EXTENSIONS = {
    ".pyc", ".pyo",
    ".exe", ".dll", ".so", ".dylib",
    ".bin", ".obj", ".o", ".class",
    ".png", ".jpg", ".jpeg", ".gif",
    ".ico", ".webp", ".bmp", ".tiff",
    ".mp3", ".mp4", ".wav", ".ogg",
    ".pdf", ".zip", ".rar", ".7z",
    ".sqlite", ".db",
    ".woff", ".woff2", ".ttf", ".otf",
}

MAX_FILE_SIZE = 1_000_000  # Skip files larger than 1 MB


def should_ignore(path: Path) -> bool:
    if path.name in IGNORED_FILES:
        return True

    if path.suffix.lower() in IGNORED_EXTENSIONS:
        return True

    # Avoid accidentally including secrets and local environment files.
    if path.name in {
        ".env",
        ".env.local",
        "id_rsa",
        "id_ed25519",
    }:
        return True

    if path.name.endswith((".pem", ".key", ".p12", ".pfx")):
        return True

    return False


def collect_files():
    for current_dir, dirs, files in os.walk(PROJECT_DIR):
        current_path = Path(current_dir)

        # Do not traverse ignored directories.
        dirs[:] = sorted(
            directory
            for directory in dirs
            if directory not in IGNORED_DIRS
            and not (current_path / directory).is_symlink()
        )

        for filename in sorted(files):
            path = current_path / filename

            if path.is_symlink() or should_ignore(path):
                continue

            try:
                if path.stat().st_size > MAX_FILE_SIZE:
                    print(f"Skipped large file: {path.relative_to(PROJECT_DIR)}")
                    continue
            except OSError:
                continue

            yield path


def build_project_context():
    count = 0

    with OUTPUT_FILE.open("w", encoding="utf-8") as output:
        output.write(f"PROJECT CONTEXT\n")
        output.write(f"Project root: {PROJECT_DIR}\n")
        output.write("=" * 100 + "\n")

        for path in collect_files():
            try:
                content = path.read_text(encoding="utf-8")

                relative_path = path.relative_to(PROJECT_DIR)

                output.write(f"\n\n{'=' * 100}\n")
                output.write(f"FILE: {relative_path}\n")
                output.write(f"{'=' * 100}\n\n")
                output.write(content)
                output.write("\n")

                count += 1
                print(f"Added: {relative_path}")

            except (UnicodeDecodeError, PermissionError, OSError):
                print(f"Skipped unreadable file: {path}")

    print(f"\nDone! Included {count} files.")
    print(f"Project context saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    build_project_context()
