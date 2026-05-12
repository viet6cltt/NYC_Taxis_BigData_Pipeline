from pydantic import BaseModel, Field
from typing import Optional

class TripRequest(BaseModel):
    """Input payload for fare prediction."""

    passenger_count:      int   = Field(..., ge=1, le=6,    example=2,   description="Number of passengers")
    trip_distance:        float = Field(..., gt=0,           example=3.5, description="Trip distance in miles")
    trip_duration_seconds: float = Field(..., gt=0,          example=900, description="Trip duration in seconds")
    pickup_hour:          int   = Field(..., ge=0, le=23,    example=14,  description="Hour of pickup (0-23)")
    pickup_day_of_week:   int   = Field(..., ge=0, le=6,    example=2,   description="Day of week (0=Mon, 6=Sun)")
    pulocation_id:        Optional[int] = Field(None, example=161, description="Pickup TLC zone ID")
    dolocation_id:        Optional[int] = Field(None, example=236, description="Dropoff TLC zone ID")


class PredictionResponse(BaseModel):
    """Response from the fare prediction endpoint."""

    predicted_fare: float  = Field(..., example=14.5,    description="Predicted fare in USD")
    model_name:     str    = Field(..., example="XGB_NYC_Fare")
    model_version:  str    = Field(..., example="3")
    currency:       str    = Field(default="USD")
