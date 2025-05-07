FROM python:3.9-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

ENV PORT=8080
ENV STREAMLIT_SERVER_PORT=${PORT}
ENV STREAMLIT_SERVER_HEADLESS=true

CMD ["streamlit", "run", "nyc-subway-app.py", "--server.port", "8080"]