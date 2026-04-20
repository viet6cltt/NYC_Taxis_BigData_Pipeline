
# Download yellow taxi trip data for the years 2025 and 2024
for year in 2024 2025
do
	for month in {01..12}
	do 
		if [ "$year" -eq 2025 ] && [ "$month" -gt 11 ]; then
			continue
		fi
		
		FILE="yellow_tripdata_${year}-${month}.parquet"
		URL="https://d37ci6vzurychx.cloudfront.net/trip-data/$FILE"
		
		echo "Downloading $FILE..."
	
		wget -c --tries=3 --timeout=30 -O ../../data/yellow_data/${year}/$FILE "$URL" || echo "Failed: $FILE"
	done
done

# Download taxi zone lookup data

FILE_TAXI_ZONE="taxi_zone_lookup.csv"
URL_TAXI_ZONE="https://d37ci6vzurychx.cloudfront.net/misc/$FILE_TAXI_ZONE"

echo "Downloading $FILE_TAXI_ZONE..."
wget -c --tries=3 --timeout=30 -O ../../data/$FILE_TAXI_ZONE "$URL_TAXI_ZONE" || echo "Failed: $FILE_TAXI_ZONE"



