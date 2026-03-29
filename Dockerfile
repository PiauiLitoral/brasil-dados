FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY servidor.py .

EXPOSE 8000

CMD ["uvicorn", "servidor:app", "--host", "0.0.0.0", "--port", "8000"]
```

Clique **"Commit changes"** (botão verde).

---

## E corrija o requirements.txt também

Clique em **`requirements.txt`** → lápis ✏️ → apague tudo e cole:
```
fastapi==0.115.0
uvicorn==0.32.0
python-multipart==0.0.12
