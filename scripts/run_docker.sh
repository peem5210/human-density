docker build -f ../Dockerfile -t human-density:latest . && docker run --name human-density --restart always -p 80:8000 human-density:latest