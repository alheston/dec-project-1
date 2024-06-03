FROM python:3.9-slim-bookworm

WORKDIR /app

COPY /app .

COPY requirements.txt .

RUN pip install -r requirements.txt

ENV APP_ID=ed4953bf
ENV API_KEY=65b15716b757bccf8668bf3c04550682
ENV SERVER_NAME=project1.c5ogaei0wdtw.us-west-1.rds.amazonaws.com
ENV DATABASE_NAME=Project1
ENV DB_USERNAME=postgres
ENV DB_PASSWORD=postgres
ENV PORT=5432
ENV LOGGING_SERVER_NAME=project1.c5ogaei0wdtw.us-west-1.rds.amazonaws.com
ENV LOGGING_DATABASE_NAME=Project1
ENV LOGGING_DB_USERNAME=postgres
ENV LOGGING_DB_PASSWORD=postgres
ENV LOGGING_PORT=5432

CMD ["python", "-m", "project.pipelines.travel_time_etl"]