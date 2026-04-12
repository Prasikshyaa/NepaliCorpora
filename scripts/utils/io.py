import json
import gzip

def write_jsonl(filepath, records):
    """
    Write list of dicts to JSONL file.
    """
    with open(filepath, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def read_jsonl(filepath):
    """
    Read JSONL file line by line.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)

def write_text(filepath, text: str):
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(text)

def read_text(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()

def read_gzip_text(filepath):
    with gzip.open(filepath, "rt", encoding="utf-8") as f:
        return f.read()
