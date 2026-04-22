from fastapi import FastAPI

app = FastAPI(title="Setu FastAPI Project")


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "FastAPI project is running"}
