"""
Utility functions for the Finance Data Transformation project.
"""

import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

def setup_logger(logger_name):
    """
    Set up and configure logger.
    
    Args:
        logger_name (str): Name of the logger
        
    Returns:
        Logger: Configured logger instance
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)
    
    return logger

def notify_stakeholders(job_name, message):
    """
    Send notification to stakeholders in case of job failure or data quality issues.
    
    Args:
        job_name (str): Name of the job
        message (str): Message to send
    """
    try:
        # This is a mock implementation - in production, replace with actual email sending logic
        # or integration with notification systems like PagerDuty, Slack, etc.
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subject = f"[ALERT] {job_name} - {timestamp}"
        
        # Log the notification
        logger = logging.getLogger("finance_transformation")
        logger.info(f"Would send notification: {subject} - {message}")
        
        # Uncomment and configure for actual email sending
        """
        # Email configuration
        sender = "alerts@company.com"
        recipients = ["finance-team@company.com", "data-engineering@company.com"]
        smtp_server = "smtp.company.com"
        smtp_port = 587
        smtp_username = "alerts@company.com"
        smtp_password = "your-password"
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = subject
        
        # Add body
        body = f"""
        Job: {job_name}
        Time: {timestamp}
        
        Message:
        {message}
        
        This is an automated message. Please do not reply.
        """
        msg.attach(MIMEText(body, 'plain'))
        
        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
        """
        
    except Exception as e:
        # Log the error but don't raise - we don't want notification failures to affect the main job
        logger = logging.getLogger("finance_transformation")
        logger.error(f"Failed to send notification: {str(e)}")