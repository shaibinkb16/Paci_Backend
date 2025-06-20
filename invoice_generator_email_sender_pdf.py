import json
import os
import smtplib
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Optional, Union
from pydantic import BaseModel, Field, validator
from dataclasses import dataclass
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import getpass
import re

# PDF generation imports
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
from reportlab.pdfgen import canvas
from reportlab.lib.colors import HexColor

# Pydantic Models
class ReceiverDetails(BaseModel):
    name: str
    email: Optional[str] = ""
    address: Optional[str] = ""
    phone: Optional[str] = ""

class OrderItem(BaseModel):
    # Handle both formats - some have item/price, others have item_code/description/unit_price
    item: Optional[str] = None
    item_code: Optional[str] = None
    description: Optional[str] = None
    item_description: Optional[str] = None  # Added this field from your data
    quantity: int
    price: Optional[float] = None
    unit_price: Optional[float] = None
    total: Optional[float] = None
    line_total: Optional[float] = None
    total_amount: Optional[float] = None  # Added this field from your data
    
    @validator('total', 'line_total', 'total_amount', always=True)
    def calculate_total(cls, v, values):
        if v is None:
            quantity = values.get('quantity', 0)
            price = values.get('price') or values.get('unit_price', 0)
            return float(quantity) * float(price)
        return v
    
    def get_item_name(self) -> str:
        """Get item name from available fields"""
        return (self.item or 
                self.description or 
                self.item_description or 
                "Unknown Item")
    
    def get_unit_price(self) -> float:
        """Get unit price from either 'price' or 'unit_price' field"""
        return self.price or self.unit_price or 0.0
    
    def get_line_total(self) -> float:
        """Get line total from available fields"""
        return (self.total or 
                self.line_total or 
                self.total_amount or 
                (self.quantity * self.get_unit_price()))

class OrderDetails(BaseModel):
    order_number: str
    items: List[OrderItem]
    total_amount: float
    currency: Optional[str] = "USD"
    delivery_date: str
    order_date: str

class EmailMetadata(BaseModel):
    sender: str
    subject: str
    date: str
    email_id: str

class ProcessedOrder(BaseModel):
    receiver_details: ReceiverDetails
    order_details: OrderDetails
    email_metadata: EmailMetadata
    source: str
    pdf_file: Optional[str] = None

# Invoice Models
class InvoiceItem(BaseModel):
    item_code: str
    description: str
    quantity: int
    unit_price: Decimal
    line_total: Decimal

class Invoice(BaseModel):
    invoice_number: str
    invoice_date: datetime
    due_date: datetime
    order_reference: str
    customer_name: str
    customer_email: str
    customer_address: str
    customer_phone: str
    items: List[InvoiceItem]
    subtotal: Decimal
    tax_rate: Decimal = Decimal('0.10')  # 10% tax
    tax_amount: Decimal
    total_amount: Decimal
    notes: str = "Thank you for your business!"

class PDFInvoiceGenerator:
    """Class to handle PDF invoice generation using ReportLab"""
    
    def __init__(self):
        self.company_info = {
            "name": "TechSolutions Inc.",
            "address": "123 Business Park, Tech City, TC 12345",
            "phone": "(555) 123-4567",
            "email": "billing@techsolutions.com",
            "tax_id": "TAX-123456789"
        }
        
        # Define colors
        self.primary_color = HexColor('#2E86AB')  # Blue
        self.secondary_color = HexColor('#A23B72')  # Purple
        self.accent_color = HexColor('#F18F01')  # Orange
        self.text_color = HexColor('#333333')  # Dark gray
        
    def create_pdf_invoice(self, invoice: Invoice, order: ProcessedOrder, filename: str) -> bool:
        """Create a professional PDF invoice"""
        try:
            # Create the PDF document
            doc = SimpleDocTemplate(
                filename,
                pagesize=A4,
                rightMargin=0.75*inch,
                leftMargin=0.75*inch,
                topMargin=1*inch,
                bottomMargin=1*inch
            )
            
            # Container for the 'Flowable' objects
            story = []
            
            # Define styles
            styles = getSampleStyleSheet()
            
            # Custom styles
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=24,
                spaceAfter=30,
                textColor=self.primary_color,
                alignment=TA_CENTER,
                fontName='Helvetica-Bold'
            )
            
            heading_style = ParagraphStyle(
                'CustomHeading',
                parent=styles['Heading2'],
                fontSize=14,
                spaceAfter=12,
                textColor=self.primary_color,
                fontName='Helvetica-Bold'
            )
            
            normal_style = ParagraphStyle(
                'CustomNormal',
                parent=styles['Normal'],
                fontSize=10,
                textColor=self.text_color,
                fontName='Helvetica'
            )
            
            bold_style = ParagraphStyle(
                'CustomBold',
                parent=styles['Normal'],
                fontSize=10,
                textColor=self.text_color,
                fontName='Helvetica-Bold'
            )
            
            # Add title
            story.append(Paragraph("INVOICE", title_style))
            story.append(Spacer(1, 20))
            
            # Company and invoice info section
            company_invoice_data = [
                ['', ''],  # Empty row for spacing
                [f'<b>{self.company_info["name"]}</b>', f'<b>Invoice Number:</b> {invoice.invoice_number}'],
                [self.company_info["address"], f'<b>Invoice Date:</b> {invoice.invoice_date.strftime("%B %d, %Y")}'],
                [f'Phone: {self.company_info["phone"]}', f'<b>Due Date:</b> {invoice.due_date.strftime("%B %d, %Y")}'],
                [f'Email: {self.company_info["email"]}', f'<b>Order Reference:</b> {invoice.order_reference}'],
                [f'Tax ID: {self.company_info["tax_id"]}', ''],
            ]
            
            company_invoice_table = Table(company_invoice_data, colWidths=[3*inch, 3*inch])
            company_invoice_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
            ]))
            
            story.append(company_invoice_table)
            story.append(Spacer(1, 30))
            
            # Bill To section
            story.append(Paragraph("BILL TO:", heading_style))
            
            bill_to_data = [
                [f'<b>{invoice.customer_name}</b>'],
                [invoice.customer_address],
                [f'Phone: {invoice.customer_phone}'],
                [f'Email: {invoice.customer_email}'],
            ]
            
            bill_to_table = Table(bill_to_data, colWidths=[6*inch])
            bill_to_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
            ]))
            
            story.append(bill_to_table)
            story.append(Spacer(1, 20))
            
            # Order Information
            sender_name, sender_email = self.extract_email_from_sender(order.email_metadata.sender)
            
            story.append(Paragraph("ORDER INFORMATION:", heading_style))
            
            order_info_data = [
                [f'<b>Original Order Date:</b> {order.order_details.order_date}'],
                [f'<b>Delivery Date:</b> {order.order_details.delivery_date}'],
                [f'<b>Order Placed By:</b> {sender_name} ({sender_email})'],
                [f'<b>Original Subject:</b> {order.email_metadata.subject}'],
            ]
            
            order_info_table = Table(order_info_data, colWidths=[6*inch])
            order_info_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
            ]))
            
            story.append(order_info_table)
            story.append(Spacer(1, 30))
            
            # Items table
            story.append(Paragraph("ITEMS:", heading_style))
            
            # Table headers
            items_data = [['Item Code', 'Description', 'Qty', 'Unit Price', 'Line Total']]
            
            # Add items
            for item in invoice.items:
                items_data.append([
                    item.item_code,
                    item.description[:40] + ('...' if len(item.description) > 40 else ''),
                    str(item.quantity),
                    f'${item.unit_price:.2f}',
                    f'${item.line_total:.2f}'
                ])
            
            items_table = Table(items_data, colWidths=[1.2*inch, 2.8*inch, 0.6*inch, 1*inch, 1*inch])
            items_table.setStyle(TableStyle([
                # Header row
                ('BACKGROUND', (0, 0), (-1, 0), self.primary_color),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('TOPPADDING', (0, 0), (-1, 0), 12),
                
                # Data rows
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('ALIGN', (1, 1), (1, -1), 'LEFT'),  # Description left-aligned
                ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),  # Numbers right-aligned
                ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
                ('TOPPADDING', (0, 1), (-1, -1), 8),
                
                # Grid
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                
                # Alternating row colors
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, HexColor('#F8F9FA')]),
            ]))
            
            story.append(items_table)
            story.append(Spacer(1, 20))
            
            # Totals section
            totals_data = [
                ['', '', '', 'Subtotal:', f'${invoice.subtotal:.2f}'],
                ['', '', '', f'Tax ({invoice.tax_rate*100:.0f}%):', f'${invoice.tax_amount:.2f}'],
                ['', '', '', '', ''],  # Empty row for spacing
                ['', '', '', 'TOTAL:', f'${invoice.total_amount:.2f}'],
            ]
            
            totals_table = Table(totals_data, colWidths=[1.2*inch, 2.8*inch, 0.6*inch, 1*inch, 1*inch])
            totals_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 2), 'Helvetica'),
                ('FONTNAME', (0, 3), (-1, 3), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 2), 10),
                ('FONTSIZE', (0, 3), (-1, 3), 12),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                
                # Total row highlighting
                ('BACKGROUND', (3, 3), (-1, 3), self.accent_color),
                ('TEXTCOLOR', (3, 3), (-1, 3), colors.white),
                ('LINEABOVE', (3, 3), (-1, 3), 2, self.accent_color),
            ]))
            
            story.append(totals_table)
            story.append(Spacer(1, 30))
            
            # Payment terms
            story.append(Paragraph("PAYMENT TERMS:", heading_style))
            
            payment_terms = [
                "• Payment is due within 30 days of invoice date",
                "• Late payments may incur additional charges",
                f"• Please reference invoice number {invoice.invoice_number} in your payment",
                "• Ensure the customer receives a copy of this invoice"
            ]
            
            for term in payment_terms:
                story.append(Paragraph(term, normal_style))
            
            story.append(Spacer(1, 20))
            
            # Notes
            if invoice.notes:
                story.append(Paragraph("NOTES:", heading_style))
                story.append(Paragraph(invoice.notes, normal_style))
                story.append(Spacer(1, 20))
            
            # Footer
            footer_text = f"Generated on: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}"
            footer_style = ParagraphStyle(
                'Footer',
                parent=styles['Normal'],
                fontSize=8,
                textColor=colors.grey,
                alignment=TA_CENTER
            )
            story.append(Paragraph(footer_text, footer_style))
            
            # Build PDF
            doc.build(story)
            return True
            
        except Exception as e:
            print(f"Error creating PDF: {e}")
            return False
    
    def extract_email_from_sender(self, sender_string: str) -> tuple:
        """Extract email and name from sender string like 'John Doe <john@example.com>'"""
        pattern = r'^(.+?)\s*<(.+?)>$'
        match = re.match(pattern, sender_string.strip())
        
        if match:
            name = match.group(1).strip()
            email = match.group(2).strip()
            return name, email
        else:
            return sender_string.strip(), sender_string.strip()

class EmailService:
    def __init__(self):
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.default_sender_email = "paccitest28@gmail.com"
        self.default_sender_password = "kcjl vmuu awmo mfah"
        self.company_info = {
            "name": "TechSolutions Inc.",
            "address": "123 Business Park, Tech City, TC 12345",
            "phone": "(555) 123-4567",
            "email": "billing@techsolutions.com",
            "tax_id": "TAX-123456789"
        }
    
    def extract_email_from_sender(self, sender_string: str) -> tuple:
        """Extract email and name from sender string like 'John Doe <john@example.com>'"""
        pattern = r'^(.+?)\s*<(.+?)>$'
        match = re.match(pattern, sender_string.strip())
        
        if match:
            name = match.group(1).strip()
            email = match.group(2).strip()
            return name, email
        else:
            return sender_string.strip(), sender_string.strip()
    
    def test_connection(self, email: str = None, password: str = None) -> bool:
        """Test Gmail connection"""
        try:
            test_email = email or self.default_sender_email
            test_password = password or self.default_sender_password
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(test_email, test_password)
            return True
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
    
    def send_invoice_email(self, recipient_email: str, invoice: 'Invoice', 
                          invoice_file_path: str, recipient_name: str,
                          sender_metadata: EmailMetadata, customer_name: str) -> bool:
        """Send invoice email with PDF attachment to the original order sender"""
        
        print(f"  📧 Sending PDF invoice to original order sender: {recipient_name} ({recipient_email})")
        
        # Use default credentials for sending
        actual_sender_email = self.default_sender_email
        actual_sender_password = self.default_sender_password
        
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = f"{self.company_info['name']} <{actual_sender_email}>"
            msg['To'] = recipient_email
            msg['Subject'] = f"Invoice {invoice.invoice_number} for Order {invoice.order_reference}"
            
            # Email body - customized for sending back to order placer
            body = f"""Dear {recipient_name},

Thank you for placing order {invoice.order_reference}. Your professional PDF invoice has been generated and is attached to this email.

INVOICE SUMMARY:
• Invoice Number: {invoice.invoice_number}
• Invoice Date: {invoice.invoice_date.strftime('%B %d, %Y')}
• Due Date: {invoice.due_date.strftime('%B %d, %Y')}
• Total Amount: ${invoice.total_amount:.2f}
• Order Reference: {invoice.order_reference}

CUSTOMER DETAILS (Bill To):
{invoice.customer_name}
{invoice.customer_address}
Phone: {invoice.customer_phone}
Email: {invoice.customer_email}

PAYMENT INSTRUCTIONS:
- Payment is due within 30 days of invoice date
- Please reference invoice number {invoice.invoice_number} in your payment
- Ensure the customer ({invoice.customer_name}) receives a copy of this invoice
- Late payments may incur additional charges

ORDER DETAILS:
- Original Order Date: {sender_metadata.date}
- Delivery Date: {invoice.due_date.strftime('%B %d, %Y')}
- Original Subject: {sender_metadata.subject}

The attached PDF invoice contains all the detailed information and can be easily printed or forwarded as needed.

If you have any questions about this invoice or need any modifications, please don't hesitate to contact us.

Best regards,
{self.company_info['name']}
Phone: {self.company_info['phone']}
Email: {self.company_info['email']}

---
This invoice was generated automatically based on your order request.
For order-related queries, please contact our billing department.
"""
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach PDF invoice file
            if os.path.exists(invoice_file_path):
                with open(invoice_file_path, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= invoice_{invoice.invoice_number}.pdf'
                    )
                    msg.attach(part)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(actual_sender_email, actual_sender_password)
                server.send_message(msg)
            
            print(f"  ✅ PDF invoice email sent successfully to {recipient_email}")
            return True
            
        except Exception as e:
            print(f"  ❌ Failed to send PDF invoice email to {recipient_email}: {e}")
            return False

class InvoiceGeneratorAgent:
    def __init__(self, json_file_path: str = r"C:\Langchain\App-2\Backend\data\purchases\email_processing_results.json"):
        self.json_file_path = json_file_path
        self.email_service = EmailService()
        self.pdf_generator = PDFInvoiceGenerator()
        self.company_info = {
            "name": "TechSolutions Inc.",
            "address": "123 Business Park, Tech City, TC 12345",
            "phone": "(555) 123-4567",
            "email": "billing@techsolutions.com",
            "tax_id": "TAX-123456789"
        }
    
    def load_orders(self) -> List[ProcessedOrder]:
        """Load and validate orders from email_processing_results.json under 'purchase_orders' key"""
        try:
            print(f"📂 Loading purchase orders from: {self.json_file_path}")
            with open(self.json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

            if "purchase_orders" not in data or not isinstance(data["purchase_orders"], list):
                print("❌ 'purchase_orders' key missing or invalid in JSON")
                return []

            orders = []
            for i, order_data in enumerate(data["purchase_orders"]):
                try:
                    order = ProcessedOrder(**order_data)
                    orders.append(order)
                    print(f"✓ Loaded purchase order {i+1}: {order.order_details.order_number}")
                except Exception as e:
                    print(f"✗ Error parsing purchase order {i+1}: {e}")
                    continue

            print(f"✅ Successfully loaded {len(orders)} purchase orders")
            return orders

        except FileNotFoundError:
            print(f"❌ File not found: {self.json_file_path}")
            return []
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error: {e}")
            return []
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return []
    
    def generate_invoice_number(self) -> str:
        """Generate unique invoice number"""
        timestamp = datetime.now().strftime("%Y%m%d")
        unique_id = str(uuid.uuid4())[:8].upper()
        return f"INV-{timestamp}-{unique_id}"
    
    def convert_order_to_invoice(self, order: ProcessedOrder) -> Invoice:
        """Convert processed order to invoice"""
        print(f"\n📋 Converting order {order.order_details.order_number} to invoice...")
        print(f"  📧 Original sender: {order.email_metadata.sender}")
        
        # Generate invoice details
        invoice_number = self.generate_invoice_number()
        invoice_date = datetime.now()
        due_date = invoice_date + timedelta(days=30)
        
        # Convert order items to invoice items
        invoice_items = []
        for i, item in enumerate(order.order_details.items):
            item_code = item.item_code or f"ITEM-{i+1:03d}"
            description = item.get_item_name()
            unit_price = Decimal(str(item.get_unit_price()))
            line_total = Decimal(str(item.get_line_total()))
            
            invoice_item = InvoiceItem(
                item_code=item_code,
                description=description,
                quantity=item.quantity,
                unit_price=unit_price,
                line_total=line_total
            )
            invoice_items.append(invoice_item)
            print(f"  ✓ Added item: {description} x{item.quantity} @ ${unit_price}")
        
        # Calculate totals
        subtotal = sum(item.line_total for item in invoice_items)
        tax_amount = subtotal * Decimal('0.10')  # 10% tax
        total_amount = subtotal + tax_amount
        
        # Create invoice - using receiver_details as the bill-to information
        invoice = Invoice(
            invoice_number=invoice_number,
            invoice_date=invoice_date,
            due_date=due_date,
            order_reference=order.order_details.order_number,
            customer_name=order.receiver_details.name,
            customer_email=order.receiver_details.email or "No email provided",
            customer_address=order.receiver_details.address or "No address provided",
            customer_phone=order.receiver_details.phone or "No phone provided",
            items=invoice_items,
            subtotal=subtotal,
            tax_amount=tax_amount,
            total_amount=total_amount
        )
        
        print(f"  💰 Subtotal: ${subtotal:.2f}")
        print(f"  💰 Tax (10%): ${tax_amount:.2f}")
        print(f"  💰 Total: ${total_amount:.2f}")
        print(f"  ✓ Invoice {invoice_number} created successfully")
        
        return invoice
    
    def save_invoice_pdf(self, invoice: Invoice, order: ProcessedOrder) -> str:
        """Save invoice as PDF file"""
        # Create invoices directory if it doesn't exist
        os.makedirs("invoices", exist_ok=True)
        
        filename = f"invoices/invoice_{invoice.invoice_number}.pdf"
        
        try:
            success = self.pdf_generator.create_pdf_invoice(invoice, order, filename)
            if success:
                print(f"  📄 PDF Invoice saved to: {filename}")
                return filename
            else:
                print(f"  ✗ Error creating PDF invoice")
                return ""
        except Exception as e:
            print(f"  ✗ Error saving PDF invoice: {e}")
            return ""
    
    def process_all_orders(self, send_emails: bool = True):
        """Main method to process all orders and generate PDF invoices"""
        print("🚀 Starting PDF Invoice Generation Process...")
        print(f"📁 Looking for file: {self.json_file_path}")
        print("📧 PDF Invoices will be sent to ORIGINAL ORDER SENDERS, not customers")
        
        # Test email connection if needed
        if send_emails:
            print(f"\n📧 Email sending is enabled")
            if not self.email_service.test_connection():
                print("⚠️  Email connection test failed. PDF invoices will be generated but not sent.")
                send_emails = False
            else:
                print("✅ Email connection test successful!")
        
        # Load orders
        orders = self.load_orders()
        
        if not orders:
            print("❌ No valid orders found. Exiting...")
            return
        
        print(f"\n📊 Processing {len(orders)} orders...")
        
        generated_invoices = []
        email_results = {'sent': 0, 'failed': 0, 'no_email': 0}
        
        # Process each order
        for i, order in enumerate(orders, 1):
            print(f"\n{'='*50}")
            print(f"Processing Order {i}/{len(orders)}")
            print(f"{'='*50}")
            print(f"Order Number: {order.order_details.order_number}")
            print(f"Customer (Bill To): {order.receiver_details.name}")
            print(f"Items Count: {len(order.order_details.items)}")
            print(f"Order Total: ${order.order_details.total_amount}")
            
            # Extract sender email instead of customer email
            sender_name, sender_email = self.email_service.extract_email_from_sender(order.email_metadata.sender)
            print(f"PDF Invoice will be sent to ORIGINAL SENDER: {sender_name} ({sender_email})")
            print(f"Original Sender String: {order.email_metadata.sender}")
            
            try:
                # Generate invoice
                invoice = self.convert_order_to_invoice(order)
                
                # Save as PDF
                filename = self.save_invoice_pdf(invoice, order)
                
                if filename:
                    invoice_info = {
                        'invoice': invoice,
                        'filename': filename,
                        'recipient_email': sender_email,
                        'recipient_name': sender_name,
                        'customer_name': order.receiver_details.name,
                        'email_sent': False,
                        'original_sender': order.email_metadata.sender
                    }
                    
                    # Send PDF email to sender
                    if send_emails and sender_email and sender_email.strip():
                        print(f"\n📧 Sending PDF invoice email to original order sender...")
                        email_sent = self.email_service.send_invoice_email(
                            recipient_email=sender_email.strip(),
                            invoice=invoice,
                            invoice_file_path=filename,
                            recipient_name=sender_name,
                            sender_metadata=order.email_metadata,
                            customer_name=order.receiver_details.name
                        )
                        
                        invoice_info['email_sent'] = email_sent
                        if email_sent:
                            email_results['sent'] += 1
                        else:
                            email_results['failed'] += 1
                    
                    elif send_emails and not sender_email:
                        print(f"  ⚠️  No sender email address found - PDF invoice saved locally only")
                        email_results['no_email'] += 1
                    
                    generated_invoices.append(invoice_info)
                
            except Exception as e:
                print(f"  ✗ Error processing order {order.order_details.order_number}: {e}")
                continue
        
        # Summary
        print(f"\n{'='*60}")
        print("PDF INVOICE PROCESSING SUMMARY")
        print(f"{'='*60}")
        print(f"Total Orders Processed: {len(orders)}")
        print(f"PDF Invoices Generated: {len(generated_invoices)}")
        print(f"Success Rate: {len(generated_invoices)/len(orders)*100:.1f}%")
        
        if send_emails:
            print(f"\nEmail Summary:")
            print(f"  ✅ PDF Emails Sent Successfully: {email_results['sent']}")
            print(f"  ❌ Email Send Failures: {email_results['failed']}")
            print(f"  ⚠️  No Email Address: {email_results['no_email']}")
        
        print(f"\n📋 Generated PDF Invoices:")
        for invoice_info in generated_invoices:
            invoice = invoice_info['invoice']
            email_status = "📧 PDF Sent to Sender" if invoice_info['email_sent'] else "💾 PDF Local Only"
            recipient_name = invoice_info['recipient_name']
            print(f"  • {invoice.invoice_number} - Customer: {invoice.customer_name} - ${invoice.total_amount:.2f} - {email_status} - Sent to: {recipient_name}")
        
        print(f"\n✅ All PDF invoices saved in 'invoices/' directory")
        print(f"📧 PDF invoices were sent to ORIGINAL ORDER SENDERS, not customers")
        return generated_invoices

def install_requirements():
    """Function to check and install required packages"""
    try:
        import reportlab
        print("✅ ReportLab is already installed")
    except ImportError:
        print("📦 Installing ReportLab for PDF generation...")
        import subprocess
        import sys
        
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "reportlab"])
            print("✅ ReportLab installed successfully!")
        except subprocess.CalledProcessError:
            print("❌ Failed to install ReportLab. Please install it manually:")
            print("   pip install reportlab")
            return False
    return True

def main():
    """Main function to run the PDF invoice generator"""
    print("PDF Invoice Generator Agent v3.0 - Professional PDF Invoices")
    print("=" * 70)
    
    # Check if ReportLab is installed
    if not install_requirements():
        print("❌ Required packages not available. Exiting...")
        return
    
    # Ask user about email sending
    print("\nOptions:")
    print("1. Generate PDF invoices and send emails to ORIGINAL ORDER SENDERS")
    print("2. Generate PDF invoices only (no emails)")
    
    
    send_emails = '1'
    
    if send_emails:
        print("\n⚠️  IMPORTANT: PDF invoices will be sent to the ORIGINAL ORDER SENDERS")
        print("   (The people who placed the orders via email)")
        print("   NOT to the customers (Bill To addresses)")
    
    # Create PDF invoice generator
    agent = InvoiceGeneratorAgent()
    
    # Process all orders
    invoices = agent.process_all_orders(send_emails=send_emails)
    
    print(f"\n🎉 PDF Invoice generation completed!")
    
    if send_emails and invoices:
        emails_sent = sum(1 for inv in invoices if inv.get('email_sent', False))
        print(f"📧 {emails_sent} PDF invoices were emailed to ORIGINAL ORDER SENDERS")
    
    print(f"📄 All PDF invoices are saved in the 'invoices/' directory")
    print(f"💡 PDF invoices are professional, printable, and easy to forward")

if __name__ == "__main__":
    main()