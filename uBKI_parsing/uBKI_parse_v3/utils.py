import os, json, random, logging

CHECKPOINT_FILE = "ubki_checkpoint.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("ubki_parser.log", encoding="utf-8")]
)
logger = logging.getLogger("UBKI")

def backoff_delay(attempt: int, base: float = 2.0, jitter: float = 1.0) -> float:
    return (base ** (attempt - 1)) + random.random() * jitter

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed": {}, "pending": {}, "retry": {}}

def save_checkpoint(state):
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)
