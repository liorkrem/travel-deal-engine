import sys
import pandas as pd
from pathlib import Path

# --- Environment Setup ---
# Initialize base directory and ensure the script can resolve local module imports
BASE_DIR = Path(__file__).parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

# --- Module Imports ---
# Import engine-specific scraping logic and centralized data processing utilities
import scripts.main_booking as main_booking
import scripts.main_agoda as main_agoda
from scripts import data_processor as dp

def run_unified_system():
    """ 
    Orchestrates the end-to-end hotel data pipeline:
    1. Triggers scraping engines for multiple platforms.
    2. Merges and deduplicates results via fuzzy matching.
    3. Applies business logic filters based on user constraints.
    4. Exports structured Excel reports for technical audit and visualization.
    """
    print("\n" + "█" * 60)
    print("    HOTEL SCRAPER SYSTEM - UNIFIED PIPELINE")
    print("█" * 60 + "\n")

    # --- User Configuration Input ---
    # Collect search parameters and business-defined filtering thresholds
    u_city = input("Enter City Name (e.g., London): ").strip()
    u_in = input("Check-in Date (YYYY-MM-DD): ").strip()
    u_out = input("Check-out Date (YYYY-MM-DD): ").strip()
    
    u_price = input("Enter max price limit (or press ENTER for no limit): ").strip()
    u_dist = input("Enter max distance from center(km) (or press ENTER for no limit): ").strip()
    u_rating = input("Enter min rating limit (or press ENTER for no limit): ").strip()
    u_reviews = input("Enter minimum reviews count (or press ENTER for no limit): ").strip()

    bookingDf = None
    agodaDf = None

    try:
        # --- Phase 1: Scraping Booking.com ---
        # Initialize the Booking.com engine and fetch available listings
        print("\n🚀 [Phase 1/3] Launching Booking.com Engine...")
        bookingDf = main_booking.run(city=u_city, checkin=u_in, checkout=u_out)
        
        if bookingDf is not None:
            print(f"✅ Booking.com data captured: {len(bookingDf)} rows.")
        
        print("\n" + "-"*40 + "\n")

        # --- Phase 2: Scraping Agoda ---
        # Initialize the Agoda engine and fetch available listings
        print("🚀 [Phase 2/3] Launching Agoda Engine...")
        agodaDf = main_agoda.run(city=u_city, checkin=u_in, checkout=u_out)
        
        if agodaDf is not None:
            print(f"✅ Agoda data captured: {len(agodaDf)} rows.")

        # --- Phase 3: Data Processing & Matching ---
        # Consolidate datasets if both engines returned valid data
        if bookingDf is not None and agodaDf is not None:
            print("\n🔍 [Phase 3/3] Processing & Matching Datasets...")
            
            # Execute cross-source matching logic to identify identical hotels across platforms
            results_df, matched_booking_idx, matched_agoda_idx = dp.match_datasets(
                bookingDf, 
                agodaDf
            )
            
            if not results_df.empty:
                # Isolate records that did not find a cross-platform match
                df_un_booking = bookingDf.drop(index=list(matched_booking_idx))
                df_un_agoda = agodaDf.drop(index=list(matched_agoda_idx))
                
                output_path = BASE_DIR / 'outputs'
                output_path.mkdir(exist_ok=True)

                # --- Technical Audit Export ---
                # Save detailed matching metrics for debugging and verification
                tech_report_file = output_path / 'matching_technical_details.xlsx'
                results_df.to_excel(tech_report_file, index=False)
                print(f"🔬 Technical Audit Log generated: {tech_report_file}")

                # --- Data Normalization ---
                # Map matched records to a unified schema (Recommended_Match only)
                m_norm = results_df[results_df['Recommended_Match'] == 'YES'][[
                    'HOTEL_NAME_Booking', 'Cheapest_Price', 'Rating_From_Max_Source', 
                    'Max_Reviews_Count', 'DISTANCE_Booking', 'Cheapest_Deal_URL', 'Cheapest_Source'
                ]].copy()
                m_norm.columns = ['Hotel_Name', 'Price', 'Rating', 'Reviews_Amount', 'Distance', 'URL', 'Original_Source']
                m_norm['Status'] = 'Matched'

                # Standardize Booking unmatched records to align with the unified schema
                b_norm = df_un_booking[['HOTEL_NAME', 'PRICE', 'RATING', 'REVIEW_AMOUNT', 'DISTANCE', 'URL']].copy()
                b_norm.columns = ['Hotel_Name', 'Price', 'Rating', 'Reviews_Amount', 'Distance', 'URL']
                b_norm['Status'], b_norm['Original_Source'] = 'Unmatched', 'Booking'

                # Standardize Agoda unmatched records to align with the unified schema
                a_norm = df_un_agoda[['HOTEL_NAME', 'PRICE', 'RATING', 'REVIEW_AMOUNT', 'DISTANCE', 'URL']].copy()
                a_norm.columns = ['Hotel_Name', 'Price', 'Rating', 'Reviews_Amount', 'Distance', 'URL']
                a_norm['Status'], a_norm['Original_Source'] = 'Unmatched', 'Agoda'

                # --- Final Report Generation ---
                # Concatenate all processed sources into a single Master DataFrame
                df_master = pd.concat([m_norm, b_norm, a_norm], ignore_index=True)
                
                # Export the complete unfiltered dataset
                raw_report_file = output_path / 'MASTER_REPORT_FULL_RAW.xlsx'
                df_master.to_excel(raw_report_file, index=False)
                print(f"✅ Raw Master Report generated: {raw_report_file}")

                # Filter dataset based on user-defined business constraints
                print(f"🎯 Applying Filters (Price <= {u_price if u_price else '∞'}, Rating >= {u_rating if u_rating else '0'})...")
                df_final = dp.filter_business_logic(df_master, u_price, u_rating, u_reviews, u_dist)

                # Export the filtered dataset for business consumption
                filtered_report_file = output_path / 'MASTER_REPORT_FILTERED.xlsx'
                df_final.to_excel(filtered_report_file, index=False)
                print(f"✅ Filtered Report generated: {filtered_report_file}")

                # Calculate proprietary value scores for data visualization tools
                INDEXED_DF = dp.calculate_hotel_value_score(df_final)

                # Export the final processed dataset optimized for Tableau/BI tools
                indexed_report_file= output_path / 'READY_FOR_VISUALIZATIONS.xlsx'
                INDEXED_DF.to_excel(indexed_report_file, index=False)

                # --- Execution Summary ---
                print(f"\n--- Processing Summary ---")
                print(f"✅ Total Potential Hotels: {len(df_master)}")
                print(f"✅ Hotels after Business Filters: {len(df_final)}")
                print(f"--------------------------\n")
            else:
                print("⚠️ No valid matches found between sources.")
        else:
            print("⚠️ One or more engines failed to return data. Skipping Phase 3.")

        print("\n" + "█" * 60)
        print("    PROCESS COMPLETE")
        print("█" * 60)

        return bookingDf, agodaDf

    except Exception as e:
        print(f"\n❌ Critical Error in Unified System: {e}")
        return None, None
    
if __name__ == "__main__":
    run_unified_system()