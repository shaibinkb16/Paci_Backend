import smtplib
import imaplib
import email
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from getpass import getpass
import os
from datetime import datetime
import json
from typing import List, Dict, Any
from langchain_groq import ChatGroq
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field
from typing import List, Optional
from dotenv import load_dotenv
import PyPDF2
import tempfile
import base64
from email.message import EmailMessage
from email.utils import make_msgid, formataddr, parseaddr
import re
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

class OrderItem(BaseModel):
    name: str = Field(description="Name of the item")
    quantity: int = Field(description="Quantity of the item")
    price: float = Field(description="Price per unit")
    total: float = Field(description="Total price for this item")

class ReceiverDetails(BaseModel):
    name: str = Field(description="Name of the receiver")
    email: str = Field(description="Email address of the receiver")
    address: str = Field(description="Full address of the receiver")
    phone: str = Field(description="Phone number of the receiver")

class OrderDetails(BaseModel):
    order_number: str = Field(description="Purchase order number")
    items: List[OrderItem] = Field(description="List of ordered items")
    total_amount: float = Field(description="Total amount of the order")
    currency: str = Field(description="Currency of the order")
    delivery_date: str = Field(description="Expected delivery date")
    order_date: str = Field(description="Date when the order was placed")

class EmailMetadata(BaseModel):
    sender: str = Field(description="Email sender")
    subject: str = Field(description="Email subject")
    date: str = Field(description="Email date")
    email_id: str = Field(description="Email ID")

class PurchaseOrder(BaseModel):
    email_metadata: EmailMetadata
    receiver_details: ReceiverDetails
    order_details: OrderDetails

class GmailAutomation:
    # Account credentials
    DEFAULT_EMAIL = "paccitest28@gmail.com"
    DEFAULT_PASSWORD = "kcjl vmuu awmo mfah"
    
    def __init__(self):
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.imap_server = "imap.gmail.com"
        self.imap_port = 993
        self.email = self.DEFAULT_EMAIL
        self.password = self.DEFAULT_PASSWORD
        self.smtp_connection = None
        self.imap_connection = None
        self.llm = None
        self.setup_llm()
        self.setup_directories()

    def setup_directories(self):
        """Create necessary directories for storing downloaded files"""
        self.base_dir = Path("reconciliation_agent")
        self.expense_dir = self.base_dir / "expenses"
        self.account_dir = self.base_dir / "statement"
        self.purchase_dir = self.base_dir / "purchases"
        
        # Create directories if they don't exist
        self.expense_dir.mkdir(parents=True, exist_ok=True)
        self.account_dir.mkdir(parents=True, exist_ok=True)
        self.purchase_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"✓ Directories created:")
        print(f"  - Expense Bills: {self.expense_dir}")
        print(f"  - Account Statements: {self.account_dir}")
        print(f"  - Purchase Orders: {self.purchase_dir}")

    def setup_llm(self):
        """Setup Groq LLM connection"""
        try:
            groq_api_key = os.getenv("GROQ_API_KEY")
            if not groq_api_key:
                print("Error: GROQ_API_KEY not found in .env file")
                return
            
            self.llm = ChatGroq(
                groq_api_key=groq_api_key,
                model_name="llama3-70b-8192"
            )
            print("LLM connection established successfully!")
        except Exception as e:
            print(f"Failed to setup LLM: {str(e)}")

    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to be safe for file system"""
        # Remove or replace invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Remove extra spaces and dots
        filename = re.sub(r'\s+', ' ', filename).strip()
        filename = re.sub(r'\.+', '.', filename)
        # Limit length
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:200-len(ext)] + ext
        return filename

    def generate_filename(self, original_filename: str, email_metadata: Dict[str, Any], file_type: str) -> str:
        """Generate a descriptive filename for downloaded files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sender = parseaddr(email_metadata.get('sender', ''))[1].split('@')[0]
        
        if original_filename:
            name, ext = os.path.splitext(original_filename)
            filename = f"{timestamp}_{sender}_{file_type}_{name}{ext}"
        else:
            filename = f"{timestamp}_{sender}_{file_type}.pdf"
        
        return self.sanitize_filename(filename)

    def download_attachments(self, email_message, email_metadata: Dict[str, Any], target_dir: Path, file_type: str) -> List[str]:
        """Download all attachments from an email to the specified directory"""
        print(f"\n=== Downloading {file_type.title()} Attachments ===")
        downloaded_files = []

        # ✅ Ensure the target directory exists
        target_dir.mkdir(parents=True, exist_ok=True)

        if not email_message.is_multipart():
            print("✗ Email is not multipart, no attachments found")
            return downloaded_files

        attachment_count = 0
        for part in email_message.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue

            original_filename = part.get_filename()
            if not original_filename:
                continue

            attachment_count += 1
            print(f"\nProcessing attachment {attachment_count}: {original_filename}")

            try:
                file_data = part.get_payload(decode=True)
                if not file_data:
                    print(f"✗ Could not decode attachment data")
                    continue

                # ✅ Use original filename for purchase orders
                if file_type in ["purchase_order", "account_statement"]:
                    safe_filename = original_filename
                elif file_type == "expense_bill":
                    safe_filename = self.sanitize_filename(f"email_{original_filename}")
                else:
                    safe_filename = self.generate_filename(original_filename, email_metadata, file_type)


                file_path = target_dir / safe_filename

                with open(file_path, 'wb') as f:
                    f.write(file_data)

                print(f"✓ Downloaded: {safe_filename}")
                print(f"  Path: {file_path}")

                downloaded_files.append(str(file_path))

            except Exception as e:
                print(f"✗ Error downloading {original_filename}: {str(e)}")

        return downloaded_files


    def is_expense_bill_email(self, subject: str) -> bool:
        """Check if email subject indicates an expense bill"""
        expense_keywords = [
            "expense bill", "expense bills", "bill", "invoice", "receipt", 
            "expense report", "expense", "reimbursement", "payment receipt",
            "billing statement", "invoice statement", "expense invoice"
        ]
        
        subject_lower = subject.lower()
        for keyword in expense_keywords:
            if keyword in subject_lower:
                return True
        return False

    def is_account_statement_email(self, subject: str) -> bool:
        """Check if email subject indicates an account statement"""
        statement_keywords = [
            "account statement", "bank statement", "statement", "monthly statement",
            "quarterly statement", "annual statement", "account summary",
            "banking statement", "financial statement", "account report"
        ]
        
        subject_lower = subject.lower()
        for keyword in statement_keywords:
            if keyword in subject_lower:
                return True
        return False

    def process_expense_bill_email(self, email_message, email_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Process expense bill email and download attachments"""
        print(f"\n{'='*50}")
        print(f"Processing Expense Bill Email")
        print(f"Subject: {email_metadata['subject']}")
        print(f"{'='*50}")
        
        # Download attachments to expense folder
        downloaded_files = self.download_attachments(
            email_message, email_metadata, self.expense_dir, "expense_bill"
        )
        
        result = {
            "email_metadata": email_metadata,
            "type": "expense_bill",
            "downloaded_files": downloaded_files,
            "download_count": len(downloaded_files),
            "status": "success" if downloaded_files else "no_attachments"
        }
        
        return result

    def process_account_statement_email(self, email_message, email_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Process account statement email and download attachments"""
        print(f"\n{'='*50}")
        print(f"Processing Account Statement Email")
        print(f"Subject: {email_metadata['subject']}")
        print(f"{'='*50}")
        
        # Download attachments to account statements folder
        downloaded_files = self.download_attachments(
            email_message, email_metadata, self.account_dir, "account_statement"
        )
        
        result = {
            "email_metadata": email_metadata,
            "type": "account_statement",
            "downloaded_files": downloaded_files,
            "download_count": len(downloaded_files),
            "status": "success" if downloaded_files else "no_attachments"
        }
        
        return result

    def process_email_with_llm(self, email_content: str) -> Dict[str, Any]:
        """Process email content using LLM to extract purchase order details"""
        if not self.llm:
            print("Error: LLM not initialized!")
            return None

        system_prompt = """You are an expert at extracting purchase order information from emails and PDFs.
        Extract the following information and return it in a valid JSON format.
        
        Required JSON structure:
        {{
            "receiver_details": {{
                "name": "",
                "email": "",
                "address": "",
                "phone": ""
            }},
            "order_details": {{
                "order_number": "",
                "items": [],
                "total_amount": 0,
                "currency": "",
                "delivery_date": "",
                "order_date": ""
            }}
        }}

        Rules:
        1. Return ONLY the JSON object, no additional text
        2. Use empty strings for missing text fields
        3. Use 0 for missing numeric fields
        4. Ensure proper JSON formatting
        5. Return empty array [] if no items found
        6. Use numbers (not strings) for numeric fields
        7. Use ISO format (YYYY-MM-DD) for dates"""

        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("user", "Extract purchase order details from this content:\n\n{email_content}")
        ])

        try:
            print("Sending prompt to LLM...")
            chain = prompt | self.llm
            response = chain.invoke({"email_content": email_content})
            print("Received response from LLM")
            
            # Clean the response to ensure it's valid JSON
            response_text = response.content.strip()
            
            # Try to find JSON object in the response
            start_idx = response_text.find('{')
            end_idx = response_text.rfind('}') + 1
            
            if start_idx == -1 or end_idx == 0:
                print("Error: No JSON object found in LLM response")
                return None
                
            json_str = response_text[start_idx:end_idx]
            
            try:
                # Parse the JSON
                order_data = json.loads(json_str)
                
                # Validate the structure
                if not isinstance(order_data, dict):
                    print("Error: LLM response is not a dictionary")
                    return None
                    
                if "receiver_details" not in order_data or "order_details" not in order_data:
                    print("Error: Missing required fields in LLM response")
                    return None
                
                print("✓ Successfully parsed and validated JSON response")
                return order_data
                
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {str(e)}")
                print("Raw response:", json_str)
                return None
                
        except Exception as e:
            print(f"Error processing with LLM: {str(e)}")
            return None

    def extract_text_from_pdf(self, pdf_data: bytes) -> str:
        """Extract text content from PDF data"""
        try:
            # Create a temporary file to store the PDF
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
                temp_file.write(pdf_data)
                temp_file_path = temp_file.name

            # Read the PDF
            text_content = ""
            with open(temp_file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page in pdf_reader.pages:
                    text_content += page.extract_text() + "\n"

            # Clean up the temporary file
            os.unlink(temp_file_path)
            return text_content
        except Exception as e:
            print(f"Error extracting text from PDF: {str(e)}")
            return ""

    def process_pdf_attachment(self, email_message, pdf_filename: str) -> Dict[str, Any]:
        """Process a PDF attachment and extract purchase order details"""
        print(f"\n=== Processing PDF: {pdf_filename} ===")
        
        try:
            # Find the PDF attachment
            pdf_data = None
            for part in email_message.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                
                filename = part.get_filename()
                if filename == pdf_filename:
                    pdf_data = part.get_payload(decode=True)
                    break
            
            if not pdf_data:
                print(f"✗ Could not find PDF data for {pdf_filename}")
                return None
            
            # Extract text from PDF
            print("Extracting text from PDF...")
            pdf_text = self.extract_text_from_pdf(pdf_data)
            
            if not pdf_text.strip():
                print("✗ No text content found in PDF")
                return None
            
            print(f"✓ Extracted {len(pdf_text)} characters from PDF")
            print("First 200 characters of extracted text:")
            print("-" * 50)
            print(pdf_text[:200] + "...")
            print("-" * 50)
            
            # Process the extracted text with LLM
            print("\nProcessing PDF content with LLM...")
            order_data = self.process_email_with_llm(pdf_text)
            
            if order_data:
                print("✓ Successfully processed PDF content")
                return order_data
            else:
                print("✗ Failed to process PDF content")
                return None
                
        except Exception as e:
            print(f"Error processing PDF: {str(e)}")
            return None

    def has_po_details_in_body(self, email_body: str) -> bool:
        """Check if email body contains purchase order details"""
        print("\n=== Checking Email Body for PO Details ===")
        
        # Key indicators that suggest actual PO details (not just references)
        required_indicators = [
            "items:", "products:", "goods:", "quantity:", "price:", "amount:",
            "total:", "delivery:", "shipping:", "payment:", "terms:"
        ]
        
        # Optional indicators that might suggest PO details
        optional_indicators = [
            "purchase order", "po", "p.o.", "order #", "order number",
            "delivery date", "total amount", "price", "quantity"
        ]
        
        # Check for required indicators
        found_required = []
        for indicator in required_indicators:
            if indicator.lower() in email_body.lower():
                found_required.append(indicator)
        
        # Check for optional indicators
        found_optional = []
        for indicator in optional_indicators:
            if indicator.lower() in email_body.lower():
                found_optional.append(indicator)
        
        # If we found required indicators, this is definitely a PO in the body
        if found_required:
            print("✓ Found required PO indicators in email body:")
            for indicator in found_required:
                print(f"  - {indicator}")
            return True
        
        # If we found optional indicators but no required ones, this might be a reference to a PO
        if found_optional:
            print("! Found only optional PO indicators in email body:")
            for indicator in found_optional:
                print(f"  - {indicator}")
            print("  This might be a reference to a PO rather than containing actual PO details")
            return False
        
        print("✗ No PO indicators found in email body")
        return False

    def extract_pdf_attachments(self, email_message) -> List[str]:
        """Extract PDF attachments from email"""
        print("\n=== Checking for PDF Attachments ===")
        pdf_files = []
        
        if email_message.is_multipart():
            for part in email_message.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                
                filename = part.get_filename()
                if filename and filename.lower().endswith('.pdf'):
                    print(f"✓ Found PDF attachment: {filename}")
                    pdf_files.append(filename)
        
        if not pdf_files:
            print("✗ No PDF attachments found")
        
        return pdf_files
    
    def send_acknowledgment_email(self, order_data: Dict[str, Any]):
        """Send an acknowledgment email reply to the original sender"""
        try:
           # Extract metadata
            metadata = order_data.get("email_metadata", {})
            original_subject = metadata.get("subject", "")
            original_sender = parseaddr(metadata.get("sender", ""))[1]

            # ✅ Extract details from nested structure
            order_number = order_data.get("order_details", {}).get("order_number", "N/A")
            order_amount = order_data.get("order_details", {}).get("total_amount", "N/A")
            customer_name = order_data.get("receiver_details", {}).get("name", "Valued Customer")

            # Prepare reply subject
            reply_subject = f"Re: {original_subject}"

            # Email body
            body = (
                f"Dear {customer_name},\n\n"
                f"Thank you for your purchase order.\n"
                f"We have received the following details:\n"
                f" - Order Number: {order_number}\n"
                f" - Order Amount: {order_amount}\n\n"
                f"Our team will now begin processing your order.\n\n"
                f"Best regards,\n"
                f"Your Company Name"
            )
            # Create the email message
            reply = EmailMessage()
            reply['Subject'] = reply_subject
            reply['From'] = formataddr(('Your Team', self.email))
            reply['To'] = original_sender
            reply.set_content(body)

            # Connect to Gmail SMTP
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(self.email, self.password)
                smtp.send_message(reply)
                print(f"\n✓ Acknowledgment email sent to {original_sender}")

        except Exception as e:
            print(f"\n✗ Failed to send acknowledgment email: {str(e)}")

    def process_purchase_order_email(self, email_message, email_id: str) -> Dict[str, Any]:
        """Process a single purchase order email with priority logic"""
        print(f"\n{'='*50}")
        print(f"Processing Email ID: {email_id.decode()}")
        print(f"{'='*50}")
        
        # Extract email metadata
        metadata = {
            "sender": email_message['from'],
            "subject": email_message['subject'],
            "date": email_message['date'],
            "email_id": email_id.decode()
        }
        print(f"\nFrom: {metadata['sender']}")
        print(f"Subject: {metadata['subject']}")
        print(f"Date: {metadata['date']}")
        
        # Step 1: Extract email body
        print("\n=== Step 1: Extracting Email Body ===")
        email_body = ""
        if email_message.is_multipart():
            for part in email_message.walk():
                if part.get_content_type() == "text/plain":
                    email_body = part.get_payload(decode=True).decode()
                    break
        else:
            email_body = email_message.get_payload(decode=True).decode()
        
        print(f"✓ Email body extracted ({len(email_body)} characters)")
        
        # Step 2: Check email body for PO details
        print("\n=== Step 2: Analyzing Email Body ===")
        has_po_in_body = self.has_po_details_in_body(email_body)
        
        # Step 3: Check for PDF attachments
        print("\n=== Step 3: Checking PDF Attachments ===")
        pdf_files = self.extract_pdf_attachments(email_message)
        
        # Decision logic
        if has_po_in_body:
            print("\n✓ Processing email body content with LLM (contains actual PO details)")
            order_data = self.process_email_with_llm(email_body)
            if order_data:
                order_data["email_metadata"] = metadata
                order_data["source"] = "email_body"
                return order_data
        elif pdf_files:
            print("\n✓ Found PDF attachment(s) - processing PDF content")
            # ✅ Download the PDFs to the purchase_orders folder (original name)
            self.download_attachments(
                email_message=email_message,
                email_metadata=metadata,
                target_dir=self.purchase_dir,
                file_type="purchase_order"
                )
            # Process each PDF attachment
            for pdf_file in pdf_files:
                order_data = self.process_pdf_attachment(email_message, pdf_file)
                if order_data:
                    order_data["email_metadata"] = metadata
                    order_data["source"] = "pdf_attachment"
                    order_data["pdf_file"] = pdf_file
                    return order_data
            
            # If no PDFs were successfully processed
            return {
                "email_metadata": metadata,
                "source": "pdf_attachment",
                "pdf_files": pdf_files,
                "status": "PDFs found but processing failed",
                "note": "Failed to extract or process content from PDF attachments"
            }
        else:
            print("\n✗ No purchase order details found in email body or attachments")
            return {
                "email_metadata": metadata,
                "source": "none",
                "status": "No purchase order details found",
                "note": "Email contains neither PO details nor PDF attachments"
            }
    
    def fetch_and_process_emails(self) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch and process emails based on subject line categories"""
        print("\n=== Processing All Email Categories ===")
        
        if not self.imap_connection:
            print("Error: Not logged in to Gmail. Please login first!")
            return {"purchase_orders": [], "expense_bills": [], "account_statements": []}

        results = {
            "purchase_orders": [],
            "expense_bills": [],
            "account_statements": []
        }

        try:
            # Select inbox
            print("\n=== Step 1: Selecting Inbox ===")
            self.imap_connection.select('INBOX')
            print("✓ Inbox selected successfully")
            
            # Search for different types of emails
            email_searches = [
                ("purchase_orders", 'UNSEEN SUBJECT "purchase order"'),
                ("expense_bills", 'UNSEEN SUBJECT "expense"'),
                ("account_statements", 'UNSEEN SUBJECT "statement"')
            ]
            
            for category, search_criteria in email_searches:
                print(f"\n=== Processing {category.replace('_', ' ').title()} ===")
                
                # Search for emails
                _, messages = self.imap_connection.search(None, search_criteria)
                email_ids = messages[0].split()
                
                if not email_ids:
                    print(f"✗ No unread {category.replace('_', ' ')} emails found")
                    continue
                
                print(f"✓ Found {len(email_ids)} unread {category.replace('_', ' ')} emails")
                
                # Process each email
                for email_id in email_ids:
                    try:
                        _, msg_data = self.imap_connection.fetch(email_id, '(RFC822)')
                        email_body = msg_data[0][1]
                        email_message = email.message_from_bytes(email_body)
                        
                        # Extract email metadata
                        metadata = {
                            "sender": email_message['from'],
                            "subject": email_message['subject'],
                            "date": email_message['date'],
                            "email_id": email_id.decode()
                        }
                        
                        # Process based on email type
                        if category == "purchase_orders":
                            result = self.process_purchase_order_email(email_message, email_id)
                            if result and result.get("order_details"):
                                self.send_acknowledgment_email(result)
                        elif category == "expense_bills":
                            result = self.process_expense_bill_email(email_message, metadata)
                        elif category == "account_statements":
                            result = self.process_account_statement_email(email_message, metadata)
                        
                        if result:
                            results[category].append(result)
                        
                        # Mark email as read
                        self.imap_connection.store(email_id, '+FLAGS', '\\Seen')
                        print(f"✓ Marked email as read")
                        
                    except Exception as e:
                        print(f"✗ Error processing email {email_id.decode()}: {str(e)}")
                        continue
            
            # Print summary
            print(f"\n=== Processing Summary ===")
            for category, items in results.items():
                print(f"✓ {category.replace('_', ' ').title()}: {len(items)} processed")
            
            return results
            
        except Exception as e:
            print(f"\n✗ Error processing emails: {str(e)}")
            return results

    def save_processing_results(self, results: Dict[str, List[Dict[str, Any]]], filename: str = None):
        """Save all processing results to a JSON file inside purchases folder"""
        try:
            results["processing_timestamp"] = datetime.now().isoformat()
            results["summary"] = {
                "purchase_orders": len(results["purchase_orders"]),
                "expense_bills": len(results["expense_bills"]),
                "account_statements": len(results["account_statements"])
            }

            if not filename:
                filename = self.purchase_dir / "email_processing_results.json"

            with open(filename, 'w') as f:
                json.dump(results, f, indent=2)

            print(f"\n✓ Processing results saved to {filename}")
        except Exception as e:
            print(f"\n✗ Failed to save processing results: {str(e)}")


    def login(self, use_default=True):
        """Login to Gmail account"""
        print("\n=== Gmail Login ===")
        
        if use_default:
            self.email = self.DEFAULT_EMAIL
            self.password = self.DEFAULT_PASSWORD
            print(f"Using default account: {self.email}")
        else:
            self.email = input("Enter your Gmail address: ")
            self.password = getpass("Enter your Gmail password or App Password: ")

        try:
            # SMTP connection
            self.smtp_connection = smtplib.SMTP(self.smtp_server, self.smtp_port)
            self.smtp_connection.starttls()
            self.smtp_connection.login(self.email, self.password)

            # IMAP connection
            self.imap_connection = imaplib.IMAP4_SSL(self.imap_server, self.imap_port)
            self.imap_connection.login(self.email, self.password)
            
            print("Successfully logged in!")
            return True
        except Exception as e:
            print(f"Login failed: {str(e)}")
            return False

    def logout(self):
        """Logout from Gmail"""
        try:
            if self.smtp_connection:
                self.smtp_connection.quit()
            if self.imap_connection:
                self.imap_connection.logout()
            print("Successfully logged out!")
        except Exception as e:
            print(f"Logout failed: {str(e)}")

def main():
    gmail = GmailAutomation()
    
    if gmail.login(use_default=True):
        # Process all types of emails
        results = gmail.fetch_and_process_emails()
        
        # Save results
        if any(results.values()):
            gmail.save_processing_results(results)
        
        gmail.logout()
    
    print("Processing complete!")
    
if __name__ == "__main__":
    print("=== Enhanced Gmail Automation Script ===")
    print("Features:")
    print("- Purchase Order Processing with LLM")
    print("- Expense Bill Download to expense_bills/ folder")
    print("- Account Statement Download to account_statements/ folder")
    print("- Automatic file organization and acknowledgment emails")
    print("\nNote: For Gmail accounts with 2-factor authentication enabled,")
    print("you'll need to use an App Password instead of your regular password.")
    main()