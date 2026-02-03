import pandas as pd
import re
from thefuzz import fuzz
from thefuzz import process
from pathlib import Path
from collections import defaultdict

# --- Utility Functions ---

def normalize_HOTEL_NAME(name):
    """
    Standardizes hotel names by removing noise words and special characters.
    Ensures consistency for string comparison across different data sources.
    """
    if not isinstance(name, str) or not name.strip():
        return ""
    
    # Convert to lowercase and strip whitespace for uniform processing
    original_clean = name.lower().strip()
    processed_name = original_clean
    
    # List of generic terms to remove to improve matching accuracy
    noise_words = ['hotel', 'resort', 'spa', 'suites', 'apartments', 'inn', 'boutique', 'luxury', 'grand', 'the']
    
    for word in noise_words:
        processed_name = re.sub(rf'\b{word}\b', '', processed_name)
    
    # Remove special characters and redundant whitespace
    processed_name = re.sub(r'[^a-zA-Z0-9\s]', '', processed_name)
    final_name = ' '.join(processed_name.split())
    
    return final_name if final_name else original_clean

def extract_numeric(val):
    """ 
    Extracts numeric value from string (currency, commas, etc.).
    Professional Standard: Handles None, empty strings, and malformed data safely.
    """
    if pd.isna(val) or val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    
    try:
        # Standardize: remove commas and whitespace for clean float conversion
        clean_val = str(val).replace(',', '').strip()
        # Regex to capture floating point numbers
        nums = re.findall(r'(\d+\.?\d*)', clean_val)
        
        if nums:
            return float(nums[0])
    except (ValueError, IndexError):
        # Graceful failure handling for non-numeric content
        pass
        
    return 0.0

# --- Core Processing Logic ---

def match_datasets(df_a, df_b, threshold=85, dist_tolerance=3.0):
    """
    Performs cross-platform matching between Booking (df_a) and Agoda (df_b).
    Uses fuzzy string matching combined with spatial distance validation.
    Ensures a 1-to-1 matching relationship for the primary result.
    """
    results = []
    matched_indices_a = set()
    matched_indices_b = set()
    
    # Prepare primary dataset with normalized search keys
    df_a = df_a.copy()
    df_a['original_idx'] = df_a.index
    df_a['norm_name'] = df_a['HOTEL_NAME'].apply(normalize_HOTEL_NAME)
    
    # Prepare secondary dataset and build lookup dictionary for O(1) retrieval
    df_b_proc = df_b.copy().reset_index()
    df_b_proc['norm_name'] = df_b_proc['HOTEL_NAME'].apply(normalize_HOTEL_NAME)
    b_norm_list = list(set(df_b_proc['norm_name'].tolist()))
    
    agoda_lookup = defaultdict(list)
    for _, row in df_b_proc.iterrows():
        agoda_lookup[row['norm_name']].append(row.to_dict())

    # Iterate through primary dataset to find potential candidates in secondary
    for row_a in df_a.itertuples(index=False):
        # Identify string similarity candidates using Levenshtein-based scoring
        raw_matches = process.extract(row_a.norm_name, b_norm_list, scorer=fuzz.token_sort_ratio, limit=None)
        
        valid_matches_info = []
        for match_item in raw_matches:
            m_name, m_score = match_item[0], match_item[1]
            
            # Apply threshold and secondary distance validation
            if m_score >= threshold:
                for row_b in agoda_lookup.get(m_name, []):
                    delta = abs(row_a.DISTANCE - row_b['DISTANCE'])
                    if delta <= dist_tolerance:
                        valid_matches_info.append({
                            'row_b': row_b,
                            'score': m_score,
                            'dist_delta': delta
                        })

        if not valid_matches_info:
            continue

        # --- Priority Selection Logic ---
        # Tiered prioritization: Exact matches (T1) -> High confidence (T2) -> Threshold matches (T3)
        best_ref = None
        t1 = [i for i, m in enumerate(valid_matches_info) if m['score'] == 100 and m['dist_delta'] <= dist_tolerance]
        t2 = [i for i, m in enumerate(valid_matches_info) if m['score'] > 90 and m['dist_delta'] <= 2.0]
        t3 = [i for i, m in enumerate(valid_matches_info) if m['score'] >= threshold]

        if t1:
            best_ref = t1[0]
        elif t2:
            best_ref = max(t2, key=lambda i: valid_matches_info[i]['score'])
        elif t3:
            best_ref = max(t3, key=lambda i: valid_matches_info[i]['score'])
        else:
            best_ref = max(range(len(valid_matches_info)), key=lambda i: valid_matches_info[i]['score'])

        # --- Tracking Matched Records ---
        # Flagging records to prevent data duplication in final union report
        winner_row_b = valid_matches_info[best_ref]['row_b']
        
        matched_indices_a.add(row_a.original_idx) 
        matched_indices_b.add(winner_row_b['index']) 

        # Merge platform data and determine optimal pricing/reviews source
        for i, match in enumerate(valid_matches_info):
            row_b = match['row_b']
            merged = {f"{k}_Booking": v for k, v in row_a._asdict().items() if k not in ['norm_name', 'original_idx']}
            merged.update({f"{k}_Agoda": v for k, v in row_b.items() if k not in ['norm_name', 'index']})
            
            # Logic to identify the lower price across platforms
            p_b, p_a = extract_numeric(merged.get('PRICE_Booking')), extract_numeric(merged.get('PRICE_Agoda'))
            if 0 < p_b < p_a or (p_b > 0 and p_a == 0):
                merged.update({'Cheapest_Price': p_b, 'Cheapest_Source': 'Booking', 'Cheapest_Deal_URL': merged.get('URL_Booking')})
            elif 0 < p_a < p_b or (p_a > 0 and p_b == 0):
                merged.update({'Cheapest_Price': p_a, 'Cheapest_Source': 'Agoda', 'Cheapest_Deal_URL': merged.get('URL_Agoda')})
            else:
                merged.update({'Cheapest_Price': p_b, 'Cheapest_Source': 'Same or Unavailable', 'Cheapest_Deal_URL': merged.get('URL_Booking')})

            # Logic to capture higher review count for better data reliability
            rev_b, rev_a = extract_numeric(merged.get('REVIEW_AMOUNT_Booking', 0)), extract_numeric(merged.get('REVIEW_AMOUNT_Agoda', 0))
            if rev_b >= rev_a:
                merged.update({'Max_Reviews_Count': rev_b, 'Max_Reviews_Source': 'Booking', 'Rating_From_Max_Source': merged.get('RATING_Booking')})
            else:
                merged.update({'Max_Reviews_Count': rev_a, 'Max_Reviews_Source': 'Agoda', 'Rating_From_Max_Source': merged.get('RATING_Agoda')})

            # Add technical metadata for audit purposes
            merged.update({
                'Match_Score': match['score'],
                'Dist_Delta': round(match['dist_delta'], 3),
                'Recommended_Match': "YES" if i == best_ref else "NO"
            })
            results.append(merged)

    return pd.DataFrame(results), matched_indices_a, matched_indices_b

def create_unified_standard_report(df_matched, df_un_booking, df_un_agoda, output_path):
    """ 
    Consolidates matched and unmatched records into a finalized reporting schema.
    Provides a standardized output for cross-platform hotel analysis.
    """
    # Select only validated matches for the unified report
    m_norm = df_matched[df_matched['Recommended_Match'] == 'YES'][[
        'HOTEL_NAME_Booking', 'Cheapest_Price', 'Rating_From_Max_Source', 
        'Max_Reviews_Count', 'DISTANCE_Booking', 'Cheapest_Deal_URL', 'Cheapest_Source'
    ]].copy()
    m_norm.columns = ['Hotel_Name', 'Price', 'Rating', 'Reviews_Amount', 'Distance', 'URL', 'Original_Source']
    m_norm['Status'] = 'Matched'

    def format_unmatched(df, source):
        # Local helper to map platform-specific headers to unified schema headers
        temp = df[['HOTEL_NAME', 'PRICE', 'RATING', 'REVIEW_AMOUNT', 'DISTANCE', 'URL']].copy()
        temp.columns = ['Hotel_Name', 'Price', 'Rating', 'Reviews_Amount', 'Distance', 'URL']
        temp['Status'], temp['Original_Source'] = 'Unmatched', source
        return temp

    # Process platforms' unique records
    b_norm = format_unmatched(df_un_booking, 'Booking')
    a_norm = format_unmatched(df_un_agoda, 'Agoda')

    # Union all segments into a master dataframe
    df_master = pd.concat([m_norm, b_norm, a_norm], ignore_index=True)
    
    return df_master

def filter_business_logic(df, price_limit=None, rating_limit=None, review_limit=None, distance_limit=None):
    """
    Applies business constraints to the integrated dataset.
    Excludes invalid records and filters based on user-defined numeric thresholds.
    """
    df_filtered = df.copy()

    # Data Quality: Remove rows missing critical business metrics
    df_filtered = df_filtered.dropna(subset=['Price', 'Rating', 'Reviews_Amount', 'Distance'])

    # Application of conditional filtering based on available constraints
    if price_limit and str(price_limit).strip():
        df_filtered = df_filtered[df_filtered['Price'] <= float(price_limit)]

    if rating_limit and str(rating_limit).strip():
        df_filtered = df_filtered[df_filtered['Rating'] >= float(rating_limit)]

    if review_limit and str(review_limit).strip():
        df_filtered = df_filtered[df_filtered['Reviews_Amount'] >= float(review_limit)]

    if distance_limit and str(distance_limit).strip():
        df_filtered = df_filtered[df_filtered['Distance'] <= float(distance_limit)]

    return df_filtered


def calculate_hotel_value_score(df, rating_weight=0.7, price_weight=0.3):
    """
    Calculates a weighted value score for hotels based on relative price and rating rankings.
    Uses Min-Max normalization to bring metrics onto a shared scale (0.1 - 1.0).
    """
    
    # Avoid SettingWithCopyWarning by explicitly copying the slice
    processed_df = df.copy()
    
    # Pre-calculation cleanup
    processed_df = processed_df.dropna(subset=['Price', 'Rating'])
    
    # Normalization bounds
    MIN_SCALE = 0.1
    MAX_SCALE = 1.0
    
    # Normalize Rating: Higher is better
    r_min, r_max = processed_df['Rating'].min(), processed_df['Rating'].max()
    processed_df['norm_rating'] = MIN_SCALE + (MAX_SCALE - MIN_SCALE) * \
                                 (processed_df['Rating'] - r_min) / (r_max - r_min)
    
    # Normalize Price: Lower is better (inverted in final calculation)
    p_min, p_max = processed_df['Price'].min(), processed_df['Price'].max()
    processed_df['norm_price'] = MIN_SCALE + (MAX_SCALE - MIN_SCALE) * \
                                (processed_df['Price'] - p_min) / (p_max - p_min)
    
    # Final Score computation (Weighting higher rating and lower price)
    processed_df['VALUE_SCORE'] = (processed_df['norm_rating'] * rating_weight) + \
                                 ((1.1 - processed_df['norm_price']) * price_weight)
    
    return processed_df

# --- Test Execution Block ---
if __name__ == "__main__":
    # Resolve relative paths for local execution environment
    script_dir = Path(__file__).resolve().parent
    output_dir = script_dir.parent / 'outputs'
    
    try:
        # Load external data sources
        df_booking = pd.read_excel(output_dir / 'final_data_booking.xlsx')
        df_agoda = pd.read_excel(output_dir / 'final_data_agoda.xlsx')

        # Trigger matching engine
        results_df, matched_booking_idx, matched_agoda_idx = match_datasets(df_booking, df_agoda)

        if not results_df.empty:
            # Persistent technical logging
            results_df.to_csv(output_dir / 'matching_technical_details.csv', index=False, encoding='utf-8-sig')
            
            # Identify and separate unique records
            df_un_booking = df_booking.drop(index=list(matched_booking_idx))
            df_un_agoda = df_agoda.drop(index=list(matched_agoda_idx))

            # Finalize unified reporting
            create_unified_standard_report(results_df, df_un_booking, df_un_agoda, output_dir)
            print(f"✅ Standalone test run complete.")
    except Exception as e:
        # Global exception handling for script execution
        print(f"❌ Test Error: {e}")