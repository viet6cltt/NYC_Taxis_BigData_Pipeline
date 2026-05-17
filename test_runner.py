import bentoml
from apps.serving.bentoml.schemas import TripRequest
from apps.serving.bentoml.service import _build_features, nyc_taxi_runner

nyc_taxi_runner.init_local()
req = TripRequest(passenger_count=1, trip_distance=3.5, trip_duration_seconds=900, pickup_hour=9, pickup_day_of_week=2, pulocation_id=162, dolocation_id=230)
X = _build_features(req)
print(nyc_taxi_runner.predict.run(X))
