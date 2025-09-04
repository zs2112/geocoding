#!/usr/bin/env python3
"""
Geocoding Script with Caching and Rate Limiting
==============================================
Usage:
    python3 geocoding_script.py addresses.json
"""

import json
import time
import logging
import argparse
import hashlib
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderQuotaExceeded


@dataclass
class GeocodeResult:
    """Data class for geocoding results"""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    formatted_address: Optional[str] = None
    cached: bool = False
    timestamp: Optional[str] = None
    error: Optional[str] = None


class GeocodingConfig:
    """Configuration class for geocoding parameters"""
    
    # Geocoding configuration
    USER_AGENT = "address-geocoder-1.0"
    REQUEST_DELAY = 1.1   # Nominatim rate limiting (1.1 seconds between consecutive requests for safety buffer)
    MAX_RETRIES = 3
    RETRY_DELAY = 2.0    
    TIMEOUT = 10
    
    # Cache configuration
    CACHE_FILE = "geocoding_cache.json"
    BATCH_SIZE = 20

    # Logging configuration
    LOG_FILE = 'geocoding.log'
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

    OUTPUT_FILE = "geocoded_addresses.json"


class GeocodeCache:
    """Handles caching of geocoding results with batch writing"""
    
    def __init__(self):
        self.cache_file = Path(GeocodingConfig.CACHE_FILE)
        self.batch_size = GeocodingConfig.BATCH_SIZE
        self.cache_data = self._load_cache()
        self.dirty_count = 0  # Track number of unsaved entries
        self.is_dirty = False  # Flag to track if cache needs saving
        self.logger = logging.getLogger(__name__)
    
    def _load_cache(self) -> Dict:
        """Load existing cache from file"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                self.logger.warning(f"Could not load cache file: {e}. Starting with empty cache.")
        return {}
    
    def _save_cache(self) -> None:
        """Save cache to file and reset dirty flags"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache_data, f, indent=2, ensure_ascii=False)
            # Reset dirty tracking after successful save
            self.dirty_count = 0
            self.is_dirty = False
            self.logger.info(f"Cache saved with {len(self.cache_data)} entries")
        except IOError as e:
            self.logger.error(f"Could not save cache file: {e}")
    
    def _generate_cache_key(self, address: str) -> str:
        """Generate a unique cache key for an address"""
        # Normalize address for consistent caching
        normalized = address.lower().strip()
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()
    
    def get(self, address: str) -> Optional[GeocodeResult]:
        """Retrieve cached geocoding result"""
        cache_key = self._generate_cache_key(address)
        
        if cache_key in self.cache_data:
            cache_entry = self.cache_data[cache_key]
            result = GeocodeResult(**cache_entry)
            result.cached = True
            return result
        
        return None
    
    def set(self, address: str, result: GeocodeResult) -> None:
        """Store geocoding result in cache with batch writing"""
        cache_key = self._generate_cache_key(address)
        result.timestamp = datetime.now().isoformat()
        result.cached = False  # Reset cached flag for storage
        
        self.cache_data[cache_key] = asdict(result)
        self.dirty_count += 1
        self.is_dirty = True
        
        # Save periodically every batch_size entries
        if self.dirty_count >= self.batch_size:
            self._save_cache()
    
    def force_save(self) -> None:
        """Force save cache regardless of batch size - call at end of processing"""
        if self.is_dirty:
            self._save_cache()


class AddressGeocoder:
    """Main geocoding class with caching and rate limiting"""
    
    def __init__(self):
        self.geocoder = Nominatim(
            user_agent=GeocodingConfig.USER_AGENT,
            timeout=GeocodingConfig.TIMEOUT,
        )
        self.cache = GeocodeCache()
        self.last_request_time = 0
        self.request_count = 0
        self.logger = logging.getLogger(__name__)
    
    def _rate_limit(self) -> None:
        """Implement rate limiting to respect API usage policy"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < GeocodingConfig.REQUEST_DELAY:
            sleep_time = GeocodingConfig.REQUEST_DELAY - time_since_last_request
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _format_address(self, address_data: Dict) -> str:
        """Format address data into a searchable string"""
        address_parts = []
        
        # Add street address
        if address_data.get('street_line_1'):
            street = str(address_data['street_line_1']).strip()
            if street:
                address_parts.append(street)
        
        # Add city
        if address_data.get('city'):
            city = str(address_data['city']).strip()
            if city:
                address_parts.append(city)
        
        # Add state (if present)
        if address_data.get('state'):
            state = str(address_data['state']).strip()
            if state:
                address_parts.append(state)

        # Add postal code
        if address_data.get('zip'):
            zip_code = str(address_data['zip']).strip()
            if zip_code:
                address_parts.append(zip_code)
        
        # Add country using country code
        if address_data.get('country_code'):
            country_code = str(address_data['country_code']).strip().upper()
            if country_code:
                address_parts.append(country_code)
        
        return ', '.join(address_parts)
    
    def _geocode_with_retry(self, address: str) -> GeocodeResult:
        """Geocode an address with retry logic"""
        for attempt in range(GeocodingConfig.MAX_RETRIES):
            try:
                self._rate_limit()
                
                location = self.geocoder.geocode(address)
                self.request_count += 1
                
                if location:
                    result = GeocodeResult(
                        latitude=location.latitude,
                        longitude=location.longitude,
                        formatted_address=location.address,
                        timestamp=datetime.now().isoformat()
                    )
                    self.logger.info(f"Successfully geocoded: {address[:50]}...")
                    return result
                else:
                    self.logger.warning(f"No location found for: {address[:50]}...")
                    return GeocodeResult(
                        error="Location not found",
                        timestamp=datetime.now().isoformat()
                    )
                    
            except GeocoderQuotaExceeded:
                self.logger.error("Geocoding quota exceeded. Stopping.")
                return GeocodeResult(
                    error="Quota exceeded",
                    timestamp=datetime.now().isoformat()
                )
                
            except (GeocoderTimedOut, GeocoderServiceError) as e:
                self.logger.warning(f"Geocoding attempt {attempt + 1} failed: {e}")
                if attempt < GeocodingConfig.MAX_RETRIES - 1:
                    time.sleep(GeocodingConfig.RETRY_DELAY * (attempt + 1))
                else:
                    return GeocodeResult(
                        error=f"Failed after {GeocodingConfig.MAX_RETRIES} attempts: {str(e)}",
                        timestamp=datetime.now().isoformat()
                    )
            
            except Exception as e:
                self.logger.error(f"Unexpected error geocoding {address[:50]}...: {e}")
                return GeocodeResult(
                    error=f"Unexpected error: {str(e)}",
                    timestamp=datetime.now().isoformat()
                )
        
        return GeocodeResult(
            error="Max retries exceeded",
            timestamp=datetime.now().isoformat()
        )
    
    def geocode_address(self, address_data: Dict) -> GeocodeResult:
        """Geocode a single address with caching (only cache successful results)"""
        # Format address for geocoding
        formatted_address = self._format_address(address_data)
        
        if not formatted_address.strip():
            return GeocodeResult(
                error="Empty or invalid address",
                timestamp=datetime.now().isoformat()
            )
        
        # Check cache first
        cached_result = self.cache.get(formatted_address)
        if cached_result:
            self.logger.info(f"Cache hit for: {formatted_address[:50]}...")
            return cached_result
        
        # Geocode and cache result
        result = self._geocode_with_retry(formatted_address)
        
        # Only cache successful results - let failures be retried on subsequent runs
        if result.error is None:
            self.cache.set(formatted_address, result)
            self.logger.debug("Cached successful geocoding result")
        else:
            self.logger.debug(f"Not caching failure: {result.error}")
        
        return result
    
    def process_addresses(self, addresses: List[Dict]) -> List[Dict]:
        """Process a list of addresses and add geocoding results"""
        total_addresses = len(addresses)
        processed_addresses = []
        
        self.logger.info(f"Processing {total_addresses} addresses...")
        
        for i, address_data in enumerate(addresses, 1):
            self.logger.info(f"Processing {i}/{total_addresses}: {str(address_data)[:100]}...")
            
            # Make a copy to avoid modifying original data
            result_data = address_data.copy()
            
            # Perform geocoding
            geocode_result = self.geocode_address(address_data)
            
            # Add geocoding results to the address data
            result_data.update({
                'geocoding': asdict(geocode_result)
            })
            
            processed_addresses.append(result_data)
            
            # Log progress
            if i % 10 == 0 or i == total_addresses:
                cache_hits = sum(1 for addr in processed_addresses 
                               if addr.get('geocoding', {}).get('cached', False))
                self.logger.info(f"Progress: {i}/{total_addresses} "
                               f"(Cache hits: {cache_hits}, API calls: {self.request_count})")
        
        return processed_addresses


def load_addresses(file_path: str) -> List[Dict]:
    """Load addresses from JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            return data
        else:
            raise ValueError("JSON file should contain a list of addresses")
            
    except FileNotFoundError:
        raise FileNotFoundError(f"Address file not found: {file_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in file {file_path}: {e}")


def save_results(results: List[Dict]) -> None:
    """Save geocoded results to JSON file"""
    logger = logging.getLogger(__name__)

    try:
        with open(GeocodingConfig.OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"Results saved to: {GeocodingConfig.OUTPUT_FILE}")
    except IOError as e:
        logger.error(f"Could not save results to {GeocodingConfig.OUTPUT_FILE}: {e}")
        raise

def setup_logging() -> None:
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format=GeocodingConfig.LOG_FORMAT,
            handlers=[
                logging.FileHandler(GeocodingConfig.LOG_FILE),
                logging.StreamHandler()
            ]
        )

def main():
    """Main function"""
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument("input_file")  
    args = parser.parse_args()
    
    input_file = args.input_file
    
    try:
        # Load addresses
        logger.info(f"Loading addresses from: {input_file}")
        addresses = load_addresses(input_file)
        logger.info(f"Loaded {len(addresses)} addresses")
        
        # Initialize geocoder
        geocoder = AddressGeocoder()
        
        # Process addresses
        start_time = time.time()
        results = geocoder.process_addresses(addresses)
        end_time = time.time()
        
        # Save results
        save_results(results)
        
        # Force save any remaining cached entries
        geocoder.cache.force_save()
        
        # Print summary
        total_time = end_time - start_time
        successful_geocodes = sum(1 for r in results 
                                if r.get('geocoding', {}).get('latitude') is not None)
        cache_hits = sum(1 for r in results 
                        if r.get('geocoding', {}).get('cached', False))
        
        logger.info("=" * 50)
        logger.info("GEOCODING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total addresses processed: {len(results)}")
        logger.info(f"Successful geocodes: {successful_geocodes}")
        logger.info(f"Cache hits: {cache_hits}")
        logger.info(f"API requests made: {geocoder.request_count}")
        logger.info(f"Total processing time: {total_time:.2f} seconds")
        logger.info(f"Average time per address: {total_time/len(results):.2f} seconds")
        logger.info(f"Results saved to: {GeocodingConfig.OUTPUT_FILE}")
        
    except Exception as e:
        logger.error(f"Error processing addresses: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    setup_logging()
    exit(main())
