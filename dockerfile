FROM python:3

WORKDIR /app
COPY . .
RUN pip install -r requirement.txt
CMD [ "python", "./index.py" ]
