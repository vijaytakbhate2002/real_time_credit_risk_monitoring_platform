"""
app.py
──────
CreditFlow — FastAPI Application

Routes
------
GET  /health              → service liveness check
POST /application-event   → validate input, publish to Kafka topic
POST /validate            → validate only, no Kafka publish (dev utility)
"""

import json
import logging

from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from pydantic import BaseModel

from src.validation import RequestValidator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "application-event-risk-monitoring"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=3,
)


app = FastAPI(title="CreditFlow API")

validator = RequestValidator()

class ApplicationEventRequest(BaseModel):
    application_id: str
    input_features: dict


@app.get("/health")
def health_check():
    return {"status": "ok", "service": "CreditFlow API"}


@app.post("/application-event", status_code=201)
def application_event(request: ApplicationEventRequest):
    """
    Validate the input payload and publish it to the Kafka topic
    `application-event-risk-monitoring`.

    Flow
    ----
    1. Validate input_features against FEATURE_SCHEMA.
    2. If invalid → 400 with list of validation errors.
    3. If valid   → publish to Kafka → 201 with ack.
    """
    result = validator.validate(request.input_features)

    if not result.is_valid:
        logger.warning(
            "Validation failed for %s | errors=%s",
            request.application_id,
            result.errors,
        )
        raise HTTPException(status_code=400, detail={"errors": result.errors})

    # Build Kafka message
    message = {
        "application_id": request.application_id,
        "input_features":  result.data,  
    }

    producer.send(
        topic=KAFKA_TOPIC,
        key=request.application_id,
        value=message,
    )
    producer.flush()

    logger.info("Published to Kafka | application_id=%s", request.application_id)

    return {
        "status":         "published",
        "application_id": request.application_id,
        "topic":          KAFKA_TOPIC,
    }


@app.post("/validate")
def validate(request: ApplicationEventRequest):
    """
    Validate only — no Kafka publish.
    Useful for dev / debugging the simulator CSV payloads.
    """
    result = validator.validate(request.input_features)

    if not result.is_valid:
        raise HTTPException(status_code=400, detail={"errors": result.errors})

    return {"status": "valid", "application_id": request.application_id}
