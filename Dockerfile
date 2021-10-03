FROM python:3.7
COPY . /app
WORKDIR /app
RUN pip3 install -r requirements.txt
RUN pip3 install fastapi_utils
CMD ["uvicorn", "backend:app", "--reload", "--host=0.0.0.0"]