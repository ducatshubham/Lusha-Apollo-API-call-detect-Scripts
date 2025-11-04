from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import os
from dotenv import load_dotenv
import csv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email import encoders

# ===================================================================
# CONFIGURATION
# ===================================================================
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

# Email Configuration
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("DEFAULT_FROM_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
DEFAULT_FROM_EMAIL = os.getenv("DEFAULT_FROM_EMAIL")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")

# ===================================================================
# EMAIL SERVICE CLASS
# ===================================================================
class EmailService:
    def __init__(self):
        self.smtp_server = SMTP_SERVER
        self.smtp_port = SMTP_PORT
        self.smtp_username = SMTP_USERNAME
        self.smtp_password = SMTP_PASSWORD
        self.default_from_email = DEFAULT_FROM_EMAIL
    
    def send_email(self, to_email: str, subject: str, body: str, from_email: str = None, 
                   cc_email: str = None, bcc_email: str = None, attachment: bytes = None, 
                   filename: str = None, is_html: bool = False):
        msg = MIMEMultipart('alternative') if is_html else MIMEMultipart()
        msg['From'] = from_email or self.default_from_email
        msg['To'] = to_email
        if cc_email:
            msg['Cc'] = cc_email
        if bcc_email:
            msg['Bcc'] = bcc_email
        msg['Subject'] = subject
        
        if is_html:
            text_part = MIMEText(body.replace('<br>', '\n').replace('<p>', '').replace('</p>', '\n'), 'plain')
            html_part = MIMEText(body, 'html')
            msg.attach(text_part)
            msg.attach(html_part)
        else:
            msg.attach(MIMEText(body, 'plain'))
        
        if attachment and filename:
            part = MIMEApplication(attachment, Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg.attach(part)
        
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)
            return True
        except Exception as e:
            print(f"Error sending email: {str(e)}")
            return False

# ===================================================================
# MONGODB CONNECTION
# ===================================================================
def connect_to_mongodb():
    """Connect to MongoDB database"""
    try:
        if not MONGO_URI or not DB_NAME or not COLLECTION_NAME:
            print("Error: Missing configuration in .env file")
            return None
        
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        client.server_info()
        
        return collection
        
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

# ===================================================================
# FETCH LAST 24 HOURS DATA
# ===================================================================
def fetch_last_24_hours_records(collection):
    """Fetch records from last 24 hours"""
    twenty_four_hours_ago = datetime.now(timezone.utc) - timedelta(hours=24)
    
    query = {
        "$or": [
            {"created_at": {"$gte": twenty_four_hours_ago}},
            {"created_at": {"$gte": twenty_four_hours_ago.isoformat()}}
        ]
    }
    
    records = list(collection.find(query))
    return records

# ===================================================================
# ANALYZE DATA
# ===================================================================
def analyze_records(records):
    """Analyze records and generate statistics"""
    
    total_calls = len(records)
    successful_calls = sum(1 for r in records if r.get('lusha_api_success') is True)
    failed_calls = sum(1 for r in records if r.get('lusha_api_success') is False)
    apollo_calls = sum(1 for r in records if r.get('enrichment_source') == 'APOLLO')
    lusha_calls = total_calls - apollo_calls
    
    phone_found_total = sum(1 for r in records if r.get('phone_found') is True)
    phone_not_found_total = sum(1 for r in records if r.get('phone_found') is False)
    apollo_phone_found_total = sum(1 for r in records if r.get('phone_found') is True and r.get('enrichment_source') == 'APOLLO')
    lusha_phone_found_total = sum(1 for r in records if r.get('phone_found') is True and r.get('enrichment_source') != 'APOLLO')
    
    # User-wise statistics
    user_stats = defaultdict(lambda: {
        'total': 0,
        'success': 0,
        'failed': 0,
        'lusha_calls': 0,
        'lusha_phone_found': 0,
        'apollo_calls': 0,
        'apollo_phone_found': 0,
        'phone_found': 0
    })
    
    for record in records:
        user_name = record.get('user_name', 'Unknown')
        stats = user_stats[user_name]
        
        stats['total'] += 1
        
        is_success = record.get('lusha_api_success') is True
        phone_found = record.get('phone_found') is True
        source = record.get('enrichment_source')
        
        if is_success:
            stats['success'] += 1
        else:
            stats['failed'] += 1
        
        if phone_found:
            stats['phone_found'] += 1
        
        if source == 'APOLLO':
            stats['apollo_calls'] += 1
            if phone_found:
                stats['apollo_phone_found'] += 1
        else:
            stats['lusha_calls'] += 1
            if phone_found:
                stats['lusha_phone_found'] += 1
    
    return {
        'overall': {
            'total_calls': total_calls,
            'successful_calls': successful_calls,
            'failed_calls': failed_calls,
            'apollo_calls': apollo_calls,
            'lusha_calls': lusha_calls,
            'phone_found_total': phone_found_total,
            'phone_not_found_total': phone_not_found_total,
            'apollo_phone_found_total': apollo_phone_found_total,
            'lusha_phone_found_total': lusha_phone_found_total
        },
        'user_stats': dict(user_stats)
    }

# ===================================================================
# GENERATE CSV
# ===================================================================
def generate_csv(analysis):
    """Generate CSV file from analysis data"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"api_performance_report_{timestamp}.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write Overall Summary
        writer.writerow(['OVERALL SUMMARY'])
        writer.writerow(['Metric', 'Value'])
        overall = analysis['overall']
        writer.writerow(['Total API Calls', overall['total_calls']])
        writer.writerow(['Successful Calls', overall['successful_calls']])
        writer.writerow(['Failed Calls', overall['failed_calls']])
        writer.writerow(['Apollo Calls', overall['apollo_calls']])
        writer.writerow(['Lusha Calls', overall['lusha_calls']])
        writer.writerow(['Phone Found Total', overall['phone_found_total']])
        writer.writerow(['Apollo Phone Found', overall['apollo_phone_found_total']])
        writer.writerow(['Lusha Phone Found', overall['lusha_phone_found_total']])
        writer.writerow(['Phone Not Found', overall['phone_not_found_total']])
        writer.writerow([])
        
        # Write User-wise Performance
        writer.writerow(['USER-WISE PERFORMANCE'])
        writer.writerow(['User', 'Total', 'Success', 'Failed', 'Lusha', 'Lusha Ph', 'Apollo', 'Apollo Ph', 'Total Ph'])
        
        user_stats = analysis['user_stats']
        for user_name, stats in sorted(user_stats.items()):
            writer.writerow([
                user_name,
                stats['total'],
                stats['success'],
                stats['failed'],
                stats['lusha_calls'],
                stats['lusha_phone_found'],
                stats['apollo_calls'],
                stats['apollo_phone_found'],
                stats['phone_found']
            ])
    
    return filename

# ===================================================================
# SEND EMAIL WITH CSV
# ===================================================================
def send_email_with_csv(csv_filename, analysis):
    """Send email with CSV attachment using EmailService"""
    try:
        if not all([SMTP_PASSWORD, DEFAULT_FROM_EMAIL, RECIPIENT_EMAIL]):
            print("Error: Missing email configuration in .env file")
            print("Required: SMTP_PASSWORD, DEFAULT_FROM_EMAIL, RECIPIENT_EMAIL")
            return False
        
        # Read CSV file
        with open(csv_filename, 'rb') as f:
            csv_data = f.read()
        
        # Email body
        overall = analysis['overall']
        body = f"""Hello,

Please find attached the 24-hour API Performance Report.

SUMMARY:
- Total API Calls: {overall['total_calls']}
- Successful: {overall['successful_calls']} | Failed: {overall['failed_calls']}
- Apollo Calls: {overall['apollo_calls']} | Lusha Calls: {overall['lusha_calls']}
- Phone Numbers Found: {overall['phone_found_total']} (Apollo: {overall['apollo_phone_found_total']}, Lusha: {overall['lusha_phone_found_total']})

Detailed report is attached as CSV file.

Best regards,
API Performance Monitor
"""
        
        # Initialize EmailService and send
        email_service = EmailService()
        success = email_service.send_email(
            to_email=RECIPIENT_EMAIL,
            subject=f"API Performance Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            body=body,
            attachment=csv_data,
            filename=csv_filename
        )
        
        return success
        
    except Exception as e:
        print(f"Error sending email: {e}")
        return False

# ===================================================================
# MAIN EXECUTION
# ===================================================================
def main():
    """Main execution function"""
    collection = connect_to_mongodb()
    
    if collection is None:
        return
    
    records = fetch_last_24_hours_records(collection)
    
    if len(records) == 0:
        return
    
    analysis = analyze_records(records)
    
    # Generate CSV
    csv_filename = generate_csv(analysis)
    
    # Send email
    send_email_with_csv(csv_filename, analysis)

# ===================================================================
# ENTRY POINT
# ===================================================================
if __name__ == "__main__":
    main()