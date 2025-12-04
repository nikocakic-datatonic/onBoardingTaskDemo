#!/usr/bin/env python3
"""
Review Data Generator Script
Generates CSV files with restaurant reviews data.
Usage: python generate_reviews.py --files 1000 --rows 100000 --min-date 2020-01-01 --max-date 2024-12-31
"""

import argparse
import csv
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
from faker import Faker
from tqdm import tqdm


class ReviewDataGenerator:
    """Generates realistic restaurant review data."""
    
    def __init__(self, min_date: datetime, max_date: datetime, seed: int = 42):
        """
        Initialize the generator.
        
        Args:
            min_date: Minimum date for reviews
            max_date: Maximum date for reviews
            seed: Random seed for reproducibility
        """
        self.faker = Faker()
        self.faker.seed_instance(seed)
        self.min_date = min_date
        self.max_date = max_date
        self.seed = seed
        
        # Pre-generate pools for consistency
        self._generate_user_pool(2000)  # 2000 unique users
        self._generate_location_pool(500)  # 500 unique locations
        
    def _generate_user_pool(self, num_users: int) -> None:
        """Generate a pool of unique users."""
        self.users = []
        user_ids = set()
        
        for _ in range(num_users):
            while True:
                user_id = str(uuid.uuid4())
                if user_id not in user_ids:
                    user_ids.add(user_id)
                    break
            
            join_date = self.faker.date_between(
                start_date=self.min_date - timedelta(days=365 * 2),
                end_date=self.min_date - timedelta(days=30)
            )
            
            self.users.append({
                'user_id': user_id,
                'first_name': self.faker.first_name(),
                'last_name': self.faker.last_name(),
                'city': self.faker.city(),
                'postal_code': int(self.faker.postcode()),
                'country': self.faker.country(),
                'join_date': join_date
            })
    
    def _generate_location_pool(self, num_locations: int) -> None:
        """Generate a pool of unique locations."""
        self.locations = []
        location_ids = set()
        
        for _ in range(num_locations):
            while True:
                location_id = str(uuid.uuid4())
                if location_id not in location_ids:
                    location_ids.add(location_id)
                    break
            
            join_date = self.faker.date_between(
                start_date=self.min_date - timedelta(days=365 * 5),
                end_date=self.min_date - timedelta(days=60)
            )
            
            self.locations.append({
                'location_id': location_id,
                'name': self.faker.company(),
                'city': self.faker.city(),
                'postal_code': int(self.faker.postcode()),
                'country': self.faker.country(),
                'owner_first_name': self.faker.first_name(),
                'owner_last_name': self.faker.last_name(),
                'join_date': join_date
            })
    
    def _generate_review_text(self) -> str:
        """Generate realistic review text."""
        templates = [
            "The food was {adjective}. {detail}",
            "{experience} experience. {detail}",
            "I would {recommend} recommend this place. {detail}",
            "The {aspect} was {adjective}. {detail}",
        ]
        
        adjectives = ['amazing', 'good', 'average', 'poor', 'terrible']
        experiences = ['Great', 'Good', 'Average', 'Poor', 'Terrible']
        recommends = ['definitely', 'probably', 'not']
        aspects = ['service', 'atmosphere', 'food quality', 'portion size', 'value']
        details = [
            "The staff was very friendly and attentive.",
            "The prices were reasonable for the quality.",
            "Would definitely come back again!",
            "The wait time was longer than expected.",
            "The ambiance was perfect for a dinner date.",
            "Portions were smaller than I expected.",
            "Excellent value for money.",
            "The menu had great variety.",
            "Very clean and well-maintained establishment.",
            "Parking was a bit challenging to find."
        ]
        
        template = self.faker.random_element(templates)
        review = template.format(
            adjective=self.faker.random_element(adjectives),
            experience=self.faker.random_element(experiences),
            recommend=self.faker.random_element(recommends),
            aspect=self.faker.random_element(aspects),
            detail=self.faker.random_element(details)
        )
        
        # Add some randomness to length
        if self.faker.random_int(1, 10) > 7:
            review += " " + self.faker.sentence()
        
        return review[:500]  # Limit length
    
    def _generate_ratings(self) -> Dict[str, int]:
        """Generate correlated ratings."""
        # Base rating influences all other ratings
        base_modifier = self.faker.random_int(-1, 1)
        
        food_rating = self.faker.random_int(1, 5)
        price_rating = max(1, min(5, food_rating + base_modifier + self.faker.random_int(-1, 1)))
        staff_rating = max(1, min(5, food_rating + base_modifier + self.faker.random_int(-1, 1)))
        
        # Overall rating is weighted average
        overall = int((food_rating * 0.4 + price_rating * 0.3 + staff_rating * 0.3) + 0.5)
        overall = max(1, min(5, overall))
        
        return {
            'food_rating': food_rating,
            'price_rating': price_rating,
            'staff_rating': staff_rating,
            'overall_rating': overall
        }
    
    def generate_review(self, review_id: str) -> Dict[str, any]:
        """Generate a single review record."""
        user = self.faker.random_element(self.users)
        location = self.faker.random_element(self.locations)
        ratings = self._generate_ratings()
        
        # Generate reviewed_at timestamp
        reviewed_at = self.faker.date_time_between(
            start_date=self.min_date,
            end_date=self.max_date
        )
        
        return {
            'review_id': review_id,
            'user_id': user['user_id'],
            'user_first_name': user['first_name'],
            'user_last_name': user['last_name'],
            'user_city': user['city'],
            'user_postal_code': user['postal_code'],
            'user_country': user['country'],
            'user_join_date': user['join_date'].strftime('%Y-%m-%d'),
            'location_id': location['location_id'],
            'location_name': location['name'],
            'location_city': location['city'],
            'location_postal_code': location['postal_code'],
            'location_country': location['country'],
            'location_owner_first_name': location['owner_first_name'],
            'location_owner_last_name': location['owner_last_name'],
            'location_join_date': location['join_date'].strftime('%Y-%m-%d'),
            'review_text': self._generate_review_text(),
            'food_rating': ratings['food_rating'],
            'price_rating': ratings['price_rating'],
            'staff_rating': ratings['staff_rating'],
            'overall_rating': ratings['overall_rating'],
            'reviewed_at': reviewed_at.strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def generate_batch(self, start_idx: int, num_rows: int) -> List[Dict[str, any]]:
        """Generate a batch of reviews."""
        batch = []
        for i in range(num_rows):
            review_id = str(uuid.uuid4())
            batch.append(self.generate_review(review_id))
        return batch


def generate_single_file(
    file_idx: int,
    num_rows: int,
    min_date: datetime,
    max_date: datetime,
    output_dir: Path,
    seed_base: int
) -> str:
    """Generate a single CSV file."""
    # Create unique seed for each file
    seed = seed_base + file_idx
    generator = ReviewDataGenerator(min_date, max_date, seed)
    
    # Generate data
    data = generator.generate_batch(file_idx * num_rows, num_rows)
    
    # Define column order in snake_case
    columns = [
        'review_id', 'user_id', 'user_first_name', 'user_last_name',
        'user_city', 'user_postal_code', 'user_country', 'user_join_date',
        'location_id', 'location_name', 'location_city', 'location_postal_code',
        'location_country', 'location_owner_first_name', 'location_owner_last_name',
        'location_join_date', 'review_text', 'food_rating', 'price_rating',
        'staff_rating', 'overall_rating', 'reviewed_at'
    ]
    
    # Create filename
    filename = output_dir / f"reviews_{file_idx:06d}.csv"
    
    # Write to CSV
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data)
    
    return str(filename)


def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(
        description='Generate restaurant review CSV files.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate_reviews.py --files 1000 --rows 100000 --min-date 2020-01-01 --max-date 2024-12-31
  python generate_reviews.py --files 10 --rows 1000 --min-date 2023-01-01 --max-date 2023-12-31 --output ./data
        """
    )
    
    parser.add_argument(
        '--files', '-f',
        type=int,
        default=1000,
        help='Number of CSV files to generate (default: 1000)'
    )
    
    parser.add_argument(
        '--rows', '-r',
        type=int,
        default=100000,
        help='Number of rows per file (default: 100000)'
    )
    
    parser.add_argument(
        '--min-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
        default='2020-01-01',
        help='Minimum date for reviews (format: YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--max-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
        default='2024-12-31',
        help='Maximum date for reviews (format: YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='./review_data',
        help='Output directory for CSV files (default: ./review_data)'
    )
    
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=4,
        help='Number of worker processes (default: 4)'
    )
    
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.files <= 0:
        parser.error("Number of files must be positive")
    if args.rows <= 0:
        parser.error("Number of rows must be positive")
    if args.min_date >= args.max_date:
        parser.error("Minimum date must be before maximum date")
    if args.workers <= 0:
        parser.error("Number of workers must be positive")
    
    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Starting generation of {args.files} files with {args.rows:,} rows each")
    print(f"Date range: {args.min_date.strftime('%Y-%m-%d')} to {args.max_date.strftime('%Y-%m-%d')}")
    print(f"Output directory: {output_dir.absolute()}")
    print(f"Total records: {args.files * args.rows:,}")
    print("-" * 60)
    
    # Generate files in parallel
    start_time = datetime.now()
    
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        
        # Submit all file generation tasks
        for file_idx in range(args.files):
            future = executor.submit(
                generate_single_file,
                file_idx,
                args.rows,
                args.min_date,
                args.max_date,
                output_dir,
                args.seed
            )
            futures.append(future)
        
        # Track progress
        completed_files = 0
        with tqdm(total=args.files, desc="Generating files") as pbar:
            for future in as_completed(futures):
                filename = future.result()
                completed_files += 1
                pbar.update(1)
                
                # Show progress every 10% or 10 files, whichever is smaller
                if completed_files % max(10, args.files // 10) == 0:
                    pbar.set_postfix({
                        'file': os.path.basename(filename),
                        'completed': f"{completed_files}/{args.files}"
                    })
    
    # Calculate statistics
    end_time = datetime.now()
    duration = end_time - start_time
    total_records = args.files * args.rows
    
    print("\n" + "=" * 60)
    print("GENERATION COMPLETE!")
    print("=" * 60)
    print(f"Total files generated: {args.files}")
    print(f"Total records: {total_records:,}")
    print(f"Total data size: ~{total_records * 500 / (1024**3):.2f} GB (estimated)")
    print(f"Time taken: {duration}")
    print(f"Speed: {total_records / duration.total_seconds():,.0f} records/second")
    print(f"Files saved to: {output_dir.absolute()}")
    
    # Create a metadata file
    metadata = {
        'generated_at': datetime.now().isoformat(),
        'num_files': args.files,
        'rows_per_file': args.rows,
        'total_records': total_records,
        'date_range': {
            'min': args.min_date.strftime('%Y-%m-%d'),
            'max': args.max_date.strftime('%Y-%m-%d')
        },
        'parameters': vars(args),
        'schema': [
            {'column': 'review_id', 'type': 'string', 'nullable': False},
            {'column': 'user_id', 'type': 'string', 'nullable': False},
            {'column': 'user_first_name', 'type': 'string', 'nullable': True},
            {'column': 'user_last_name', 'type': 'string', 'nullable': True},
            {'column': 'user_city', 'type': 'string', 'nullable': True},
            {'column': 'user_postal_code', 'type': 'integer', 'nullable': True},
            {'column': 'user_country', 'type': 'string', 'nullable': True},
            {'column': 'user_join_date', 'type': 'date', 'nullable': True},
            {'column': 'location_id', 'type': 'string', 'nullable': False},
            {'column': 'location_name', 'type': 'string', 'nullable': True},
            {'column': 'location_city', 'type': 'string', 'nullable': True},
            {'column': 'location_postal_code', 'type': 'integer', 'nullable': True},
            {'column': 'location_country', 'type': 'string', 'nullable': True},
            {'column': 'location_owner_first_name', 'type': 'string', 'nullable': True},
            {'column': 'location_owner_last_name', 'type': 'string', 'nullable': True},
            {'column': 'location_join_date', 'type': 'date', 'nullable': True},
            {'column': 'review_text', 'type': 'string', 'nullable': True},
            {'column': 'food_rating', 'type': 'integer', 'nullable': False, 'range': '1-5'},
            {'column': 'price_rating', 'type': 'integer', 'nullable': False, 'range': '1-5'},
            {'column': 'staff_rating', 'type': 'integer', 'nullable': False, 'range': '1-5'},
            {'column': 'overall_rating', 'type': 'integer', 'nullable': False, 'range': '1-5'},
            {'column': 'reviewed_at', 'type': 'timestamp', 'nullable': False}
        ]
    }
    
    import json
    metadata_file = output_dir / 'generation_metadata.json'
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    print(f"\nMetadata saved to: {metadata_file}")


if __name__ == '__main__':
    main()
