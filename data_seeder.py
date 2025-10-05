import mlcroissant as mlc
import pandas as pd
import os

# Fetch the Croissant JSON-LD
croissant_dataset = mlc.Dataset('https://www.kaggle.com/datasets/yashdevladdha/uber-ride-analytics-dashboard/croissant/download')

# Get record sets
record_sets = croissant_dataset.metadata.record_sets

# Process each record set
for record_set in record_sets:
    # Fetch records and create DataFrame
    df = pd.DataFrame(croissant_dataset.records(record_set=record_set.uuid))
    
    if not df.empty:
        # Clean column names
        df.columns = [col.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').lower() 
                     for col in df.columns]
        
        # Handle missing values
        df = df.fillna('NULL')
        
        # Create output directory
        os.makedirs('data/kaggle', exist_ok=True)
        
        # Save to CSV
        filename = f"{record_set.name.replace(' ', '_').replace('-', '_').lower()}.csv"
        df.to_csv(f'data/kaggle/{filename}', index=False)
