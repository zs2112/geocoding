# Geocoding Script

A Python script for batch geocoding addresses using OpenStreetMap's Nominatim service with caching and rate limiting.

> **Note**: This is a development version which will undergo further modifications.

## Quick Start

### Prerequisites
- Python 3.7+
- GeoPy library: `pip install geopy`

### Usage
```bash
python3 geocoding_script.py addresses.json
```

## Input Format

JSON file with address objects:

```json
[
  {
    "id": "unique-id",
    "street_line_1": "Street Address",
    "city": "City",
    "zip": "Postal Code", 
    "state": "State (optional)",
    "country_code": "Country Code"
  }
]
```

## Output

Creates `geocoded_addresses.json` with original data plus geocoding results:

```json
{
  "geocoding": {
    "latitude": 38.7436057,
    "longitude": -9.1531108,
    "formatted_address": "Full formatted address",
    "cached": false,
    "timestamp": "2025-09-02T15:30:45.123456",
    "error": null
  }
}
```

## Features

- **Caching**: Successful results are cached to avoid repeat API calls
- **Rate limiting**: Respects OpenStreetMap's 1 request/second policy

## Configuration

Modify settings in the `GeocodingConfig` class:

- `REQUEST_DELAY`: Time between consecutive requests (default: 1.1 seconds)
- `BATCH_SIZE`: Cache write frequency (default: 20 entries)

## Important Notes

- Respects OpenStreetMap's [usage policy](https://operations.osmfoundation.org/policies/nominatim/)
- Only successful geocoding results are cached
- Failed addresses are retried on subsequent script runs

## Files Created

- `geocoded_addresses.json` - Results file
- `geocoding_cache.json` - Cache file
- `geocoding.log` - Processing log

---
