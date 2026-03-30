from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

BASE_DIR = Path(__file__).resolve().parent
app = FastAPI()


@app.get("/")
async def root():
    return FileResponse(BASE_DIR / "miniapp" / "index.html")
