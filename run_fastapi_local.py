import uvicorn
import sys
from pathlib import Path

FASTAPI_DIR = Path(__file__).resolve().parent / "apps" / "serving" / "fastapi"
sys.path.insert(0, str(FASTAPI_DIR))

from app.main import app

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
