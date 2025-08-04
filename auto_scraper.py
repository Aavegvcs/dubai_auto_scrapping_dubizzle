import os
import subprocess
import schedule
import time
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import logging
from pytz import timezone

# Configure logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/auto_scraper.log', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'config_folder': 'config',
    'output_folder': 'output',
    'scripts': {
        'dubizzle': 'dubizzle.py',
        'invygo': 'invygo.py'
    },
    'output_files': [
        'output/dubizzle_rentals.xlsx',
        'output/invygo_rentals.xlsx'
    ],
   'email': {
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587,
    'sender_email': 'jagdish.kuri@aaveg.com',
    'sender_password': 'aidixhotyfxwiiiv',
    'receiver_emails': [
       'k.lyang@wtimobility.ae', 
       'ajay.vij@wtimobility.ae', 
       'jagdish.kuri@aaveg.com',
       'sarthak.tyagi@asndtechnology.com',
       'jatin.bhardwaj@aaveg.com'
    ]
}

}

def send_email_with_attachments(subject, body, files):
    try:
        msg = MIMEMultipart()
        msg['From'] = CONFIG['email']['sender_email']
        msg['To'] = ', '.join(CONFIG['email']['receiver_emails']) 
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        for file_path in files:
            if os.path.exists(file_path):
                with open(file_path, 'rb') as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                
                encoders.encode_base64(part)
                
                filename = os.path.basename(file_path)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename={filename}'
                )
                msg.attach(part)
                logger.info(f"Attached file: {file_path}")
            else:
                logger.warning(f"File not found for attachment: {file_path}")

        with smtplib.SMTP(CONFIG['email']['smtp_server'], CONFIG['email']['smtp_port']) as server:
            server.starttls()
            server.login(CONFIG['email']['sender_email'], CONFIG['email']['sender_password'])
            server.send_message(msg)
        
        logger.info("Email sent successfully")
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")

def run_scrapers():
    logger.info("Starting scheduled scraper run")
    executed = []
    
    # Check if make_model.csv exists
    config_file = os.path.join(CONFIG['config_folder'], 'make_model.csv')
    if not os.path.exists(config_file):
        logger.error(f"Config file {config_file} not found")
        return

    try:
        for script_name, script_file in CONFIG['scripts'].items():
            logger.info(f"Running script: {script_file}")
            try:
                subprocess.run(['python', script_file], check=True)
                executed.append(script_name)
                logger.info(f"Successfully ran {script_name}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Error running {script_name}: {str(e)}")

        if executed:
            # Send email with output files
            subject = f"Scraper Output - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            body = f"Scraping from dubizzle and invygo: {', '.join(executed).capitalize()}\n\nAttached are the output files."
            send_email_with_attachments(subject, body, CONFIG['output_files'])
        else:
            logger.warning("No scripts executed successfully")

    except Exception as e:
        logger.error(f"Fatal error in run_scrapers: {str(e)}")

def schedule_scrapers():
    india_tz = timezone('Asia/Kolkata')
    
    def job():
        now = datetime.now(india_tz)
        logger.info(f"Running scheduled job at {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        run_scrapers()

    schedule.every().day.at("09:00", india_tz).do(job)
    schedule.every().day.at("15:00", india_tz).do(job)
    logger.info("Scheduled scrapers for 9 AM and 3 PM Dubai time")

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    os.makedirs(CONFIG['config_folder'], exist_ok=True)
    os.makedirs(CONFIG['output_folder'], exist_ok=True)
    schedule_scrapers()