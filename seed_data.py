#!/usr/bin/env python3
"""
Standalone script to seed the database with sample datasets
Run this script to populate the marketplace with sample data
"""

if __name__ == "__main__":
    print("=" * 60)
    print("  Dataset Bazar - Sample Data Seeder")
    print("=" * 60)
    print()
    
    from init_sample_data import create_sample_datasets
    create_sample_datasets()
    
    print()
    print("=" * 60)
    print("  ✨ Sample data initialization complete!")
    print("  🌐 Start the server and visit the marketplace")
    print("=" * 60)
