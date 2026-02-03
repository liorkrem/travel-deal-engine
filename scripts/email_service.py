import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv  # New import for security

load_dotenv()

def send_top_hotels_email(df, recipient_email, sender_email, sender_password):
    """
    Extracts top 10 rated hotels and sends an HTML email with a compact, 
    rounded table and beige headers.
    """
    if df.empty:
        # Avoid processing if there is no data
        print("⚠️ DataFrame is empty. Nothing to send.")
        return

    # 1. Data Processing: Sort by Rating (High to Low) and Price (Low to High)
    top_10 = df.sort_values(by=['VALUE_SCORE'], ascending=[False]).head(10).copy()
    
    # Create a stylized 'View Deal' button for the email
    top_10['Link'] = top_10['URL'].apply(
        lambda x: f'<a href="{x}" style="color: #ffffff; background-color: #8e7d5a; padding: 6px 12px; text-decoration: none; border-radius: 15px; font-size: 11px; font-weight: bold;">View Deal</a>'
    )
    
    # Prepare the subset of data for the email table
    email_data = top_10[['Hotel_Name', 'Price', 'Rating', 'Reviews_Amount', 'Link']]
    email_data.columns = ['Hotel Name', 'Price', 'Rating', 'Reviews', 'Deal Link']
    
    # Convert DataFrame to HTML without escaping tags to keep the link buttons active
    html_table = email_data.to_html(index=False, escape=False, border=0)
    
    # 2. HTML and CSS Content
    subject = "Top 10 Hotel Recommendations"
    body = f"""
    <html>
        <head>
            <style>
                body {{ 
                    font-family: 'Segoe UI', Arial, sans-serif; 
                    margin: 0; padding: 20px; color: #333;
                }}
                table {{ 
                    width: 100%;
                    border-collapse: separate; 
                    border-spacing: 0;
                    margin: 20px 0;
                    font-size: 14px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.05);
                    border-radius: 12px;
                    overflow: hidden;
                }}
                th {{ 
                    background-color: #f5f5dc;
                    color: #5d5d5a;
                    padding: 12px; 
                    text-align: left;
                    font-weight: 700;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    border-bottom: 2px solid #e5e5d0;
                }}
                td {{ 
                    padding: 12px; 
                    border-bottom: 1px solid #eee;
                    line-height: 1.4;
                }}
                tr:last-child td {{ border-bottom: none; }}
                tr:nth-child(even) {{ background-color: #fafafa; }}
                tr:hover {{ background-color: #f1f2f6; }}
                h2 {{ color: #2d3436; font-size: 20px; }}
            </style>
        </head>
        <body>
            <h2 style="text-align: center; color: #444; font-size: 22px; margin-bottom: 20px;">Your Personalized Top 10 Hotel Deals</h2>
            {html_table}
            <br>
            <p style="font-size: 0.8em; color: #95a5a6;">
                <i>This report was generated automatically. Prices and availability are subject to change.</i>
            </p>
        </body>
    </html>
    """

    # 3. Secure SMTP Connection and Sending
    try:
        # Establish a secure SSL connection with Gmail
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(sender_email, sender_password)
        
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))
        
        server.send_message(msg)
        server.quit()
        print(f"📧 Successfully sent compact styled report to {recipient_email}")
        
    except Exception as e:
        # Log any errors encountered during the process
        print(f"❌ SMTP Error: {e}")
        
if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parent.parent
    FILE_PATH = BASE_DIR / 'outputs' / 'READY_FOR_VISUALIZATIONS.xlsx'

    if FILE_PATH.exists():
        df_final = pd.read_excel(FILE_PATH)
        
        # Pulling credentials from environment variables instead of hardcoding
        S_MAIL = os.getenv("EMAIL_USER")
        # Use the 16-character App Password generated from Google Security settings
        # Format: abcd efgh ijkl mnop
        S_PASS = os.getenv("EMAIL_PASS")
        R_MAIL = os.getenv("EMAIL_RECEIVER")

        if S_MAIL and S_PASS:
            send_top_hotels_email(df_final, R_MAIL, S_MAIL, S_PASS)
        else:
            print("❌ Error: Missing credentials in .env file.")
    else:
        print(f"❌ Error: Could not find {FILE_PATH}")