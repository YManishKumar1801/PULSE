import os
import re
import subprocess
import sys

SKIP_DIRS = {".git", "venv", ".venv", "__pycache__", "node_modules", ".idea", ".vscode"}
SKIP_FILES = {".env.example", "check_security.py", "test_security.py"}
TEXT_EXTENSIONS = {".py", ".md", ".txt", ".toml", ".yml", ".yaml", ".json", ".cfg", ".ini", ".env", ".html", ".js", ".sh", ".ps1"}
MAX_FILE_BYTES = 2_000_000
SENSITIVE_TRACKED = (".env", "secrets.toml", ".session", ".session-journal")
REQUIRED_IGNORE_ENTRIES = (".env", "*.session", ".streamlit/secrets.toml")

PATTERNS = (
    ("Google API key", re.compile(r"AIza[0-9A-Za-z_\-]{35}")),
    ("MongoDB URI with credentials", re.compile(r"mongodb(?:\+srv)?://[^/\s:@\"']+:[^@\s\"']+@")),
    ("Private key block", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |)PRIVATE KEY-----")),
    (
        "Hardcoded secret assignment",
        re.compile(r"""(?i)\b(?:api[_-]?key|api[_-]?hash|secret|token|password)\b\s*[:=]\s*["'][A-Za-z0-9_\-/+=]{16,}["']"""),
    ),
    ("Telegram api_hash", re.compile(r"""(?i)api_hash\s*[:=]\s*["'][0-9a-f]{32}["']""")),
)


def mask(value):
    value = value.strip()
    if len(value) <= 8:
        return "*" * len(value)
    return value[:4] + "..." + "*" * 4


def scan_text(text):
    findings = []
    for number, line in enumerate(text.splitlines(), start=1):
        for label, pattern in PATTERNS:
            match = pattern.search(line)
            if match:
                findings.append({"line": number, "type": label, "sample": mask(match.group(0))})
                break
    return findings


def iter_files(root):
    for folder, dirs, files in os.walk(root):
        dirs[:] = [name for name in dirs if name not in SKIP_DIRS]
        for name in files:
            if name in SKIP_FILES:
                continue
            extension = os.path.splitext(name)[1].lower()
            if extension in TEXT_EXTENSIONS or name == ".env":
                path = os.path.join(folder, name)
                try:
                    if os.path.getsize(path) <= MAX_FILE_BYTES:
                        yield path
                except OSError:
                    continue


def scan_tree(root):
    results = []
    for path in iter_files(root):
        try:
            with open(path, encoding="utf-8", errors="ignore") as handle:
                text = handle.read()
        except OSError:
            continue
        if os.path.basename(path) == ".env":
            continue
        for finding in scan_text(text):
            results.append({"file": os.path.relpath(path, root), **finding})
    return results


def tracked_sensitive_files(root):
    try:
        output = subprocess.run(
            ["git", "ls-files"], cwd=root, capture_output=True, text=True, timeout=20, check=True
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    flagged = []
    for path in output.splitlines():
        name = os.path.basename(path)
        if name == ".env.example":
            continue
        if any(name == item or name.endswith(item) for item in SENSITIVE_TRACKED):
            flagged.append(path)
    return flagged


def missing_ignore_entries(root):
    path = os.path.join(root, ".gitignore")
    if not os.path.exists(path):
        return list(REQUIRED_IGNORE_ENTRIES)
    with open(path, encoding="utf-8", errors="ignore") as handle:
        entries = {line.strip() for line in handle if line.strip() and not line.startswith("#")}
    return [entry for entry in REQUIRED_IGNORE_ENTRIES if entry not in entries]


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    problems = 0

    findings = scan_tree(root)
    if findings:
        problems += len(findings)
        print("Possible secrets in source files:")
        for item in findings:
            print(f"  {item['file']}:{item['line']}  {item['type']}  ({item['sample']})")
    else:
        print("No hardcoded secrets found in source files.")

    missing = missing_ignore_entries(root)
    if missing:
        problems += len(missing)
        print("These entries are missing from .gitignore:", ", ".join(missing))
    else:
        print(".gitignore covers .env, *.session and .streamlit/secrets.toml.")

    tracked = tracked_sensitive_files(root)
    if tracked is None:
        print("Git was not available, so tracked files were not checked.")
    elif tracked:
        problems += len(tracked)
        print("Sensitive files are tracked by git:", ", ".join(tracked))
        print("Run: git rm --cached <file>, commit, and rotate any key that was ever committed.")
    else:
        print("No sensitive files are tracked by git.")

    print("Note: a key that was committed once stays in git history. Rotate it even after removing the file.")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()