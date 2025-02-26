import json
import os
from typing import List, Any, Iterable
from pathlib import Path
import glob
from typing import AnyStr


def find_handles_with_pattern_recursively(path: str, pattern: str) -> list[str]:
    # Find all .txt files in the current directory and subdirectories
    files: list[str] = glob.glob(os.path.join(path, "**", f"*{pattern}*"), recursive=True)
    return files


def find_files_with_handle(handle: str, folder: str) -> List[str]:
    folder_files: list[str] = os.listdir(folder)
    files = [f for f in folder_files if f"_{handle}_".lower() in f.lower() or (
            f.lower().startswith(f"{handle}") and f"{handle}_".lower() in f.lower())]  # compare by lowercase
    return [os.path.join(folder, f) for f in files]


def open_jsonl(file: str) -> List[dict]:
    data: list[dict] = []
    with open(file, "r") as f:
        for l in f.readlines():
            data.append(json.loads(l))
    return data


def save_to_jsonl(path: str, data: Iterable[Any]) -> None:
    file = Path(path)
    file.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        for item in data:
            if isinstance(item, dict):
                json.dump(item, f)
            elif isinstance(item, str):
                f.write(item)
            f.write('\n')