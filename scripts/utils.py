import re
import logging
import pandas as pd
import os

# Setup logging for the utility module
logger = logging.getLogger(__name__)

def extract_distance_km(distance_str):
    """ 
    Parses distance strings into float (KM). 
    Standardizes location data across different languages (Hebrew/English).
    """
    try:
        if not distance_str or distance_str == "N/A":
            return 0.0
        
        # Search for digits and potential decimal point
        match = re.search(r"(\d+\.?\d*)", str(distance_str))
        if not match:
            return 0.0
        
        value = float(match.group(1))
        
        # Check if the unit is KM or Meters (Supports Hebrew ק"מ/מטר)
        lower_str = str(distance_str).lower()
        if any(unit in lower_str for unit in ["km", "ק\"מ"]):
            return value
        
        # If it's meters or unspecified, convert to KM
        return value / 1000
    except Exception as e:
        logger.error(f"Error parsing distance: {e}")
        return 0.0

def finalize_report(input_csv, output_file):
    """
    Cleans raw data by removing duplicates based on dynamic column names.
    Ensures that columns like 'HOTEL_NAME_BOOKING' or 'PRICE_AGODA' are recognized.
    """
    if not os.path.exists(input_csv):
        logger.error(f"Input file {input_csv} not found.")
        return

    try:
        # Loading the raw scraped data
        df = pd.read_csv(input_csv)
        
        if df.empty:
            logger.warning(f"The file {input_csv} is empty. Nothing to process.")
            return

        # Professional Dynamic Matching:
        # Find the actual column names in the file that represent Name and Price
        name_cols = [c for c in df.columns if 'NAME' in c.upper()]
        price_cols = [c for c in df.columns if 'PRICE' in c.upper()]

        # Build a list of columns to check for duplicates
        dedup_subset = []
        if name_cols: dedup_subset.append(name_cols[0])
        if price_cols: dedup_subset.append(price_cols[0])

        # Remove duplicates if columns were found
        if dedup_subset:
            df_clean = df.drop_duplicates(subset=dedup_subset).reset_index(drop=True)
        else:
            # Fallback if no matching columns found
            df_clean = df.drop_duplicates().reset_index(drop=True)
        
        # Save as Excel or CSV depending on the extension
        if output_file.lower().endswith('.xlsx'):
            df_clean.to_excel(output_file, index=False)
        else:
            # Using utf-8-sig for Hebrew support in CSV
            df_clean.to_csv(output_file, index=False, encoding='utf-8-sig')
            
        logger.info(f"Report Finalized: {len(df_clean)} unique entries saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Failed to finalize report: {e}")