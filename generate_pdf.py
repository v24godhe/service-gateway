from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
import os
from datetime import datetime

class ServiceGatewayPDF:
    def __init__(self, filename="Service_Gateway_Documentation.pdf"):
        self.filename = filename
        self.doc = SimpleDocTemplate(filename, pagesize=A4, 
                                   rightMargin=72, leftMargin=72,
                                   topMargin=72, bottomMargin=18)
        self.styles = getSampleStyleSheet()
        self.story = []
        
        # Custom styles
        self.title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            alignment=TA_CENTER,
            textColor=colors.darkblue
        )
        
        self.heading_style = ParagraphStyle(
            'CustomHeading',
            parent=self.styles['Heading2'],
            fontSize=16,
            spaceAfter=12,
            spaceBefore=20,
            textColor=colors.darkblue
        )
    
    def generate_complete_documentation(self):
        # Cover Page
        self.add_cover_page()
        
        # Table of Contents
        self.add_table_of_contents()
        
        # System Overview
        self.add_system_overview()
        
        # Installation Guide
        self.add_installation_guide()
        
        # API Reference
        self.add_api_reference()
        
        # Maintenance Guide
        self.add_maintenance_guide()
        
        # Troubleshooting
        self.add_troubleshooting()
        
        # Build PDF
        self.doc.build(self.story)
        print(f"✅ PDF generated: {self.filename}")
    
    def add_cover_page(self):
        # Title
        self.story.append(Spacer(1, 2*inch))
        self.story.append(Paragraph("Service Gateway", self.title_style))
        self.story.append(Paragraph("Complete System Documentation", self.heading_style))
        
        # Subtitle
        self.story.append(Spacer(1, 1*inch))
        self.story.append(Paragraph("Customer Service Database Integration", self.styles['Normal']))
        self.story.append(Paragraph("AS400 to Azure MCP Gateway", self.styles['Normal']))
        
        # Version info
        self.story.append(Spacer(1, 2*inch))
        version_data = [
            ['Version', '1.0.0'],
            ['Date', datetime.now().strftime('%Y-%m-%d')],
            ['Environment', 'Production'],
            ['Server', '192.168.1.42:8080'],
            ['Database', 'AS400 (IBM i)']
        ]
        
        version_table = Table(version_data, colWidths=[2*inch, 3*inch])
        version_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.blackColor),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        self.story.append(version_table)
        self.story.append(PageBreak())
    
    def add_table_of_contents(self):
        self.story.append(Paragraph("Table of Contents", self.title_style))
        
        toc_data = [
            ['1. System Overview', '3'],
            ['2. Architecture', '4'],
            ['3. Installation Guide', '5'],
            ['4. API Reference', '8'],
            ['5. Maintenance Guide', '12'],
            ['6. Troubleshooting', '16'],
            ['7. Security', '19'],
            ['8. Performance', '21'],
            ['Appendices', '23']
        ]
        
        toc_table = Table(toc_data, colWidths=[5*inch, 1*inch])
        toc_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8)
        ]))
        
        self.story.append(toc_table)
        self.story.append(PageBreak())
    
    def add_system_overview(self):
        self.story.append(Paragraph("1. System Overview", self.title_style))
        
        overview_text = """
        The Service Gateway is a secure FastAPI-based service that provides controlled access 
        to internal AS400 database systems from cloud-based applications. It serves as a 
        bridge between Azure MCP servers and internal customer data.
        
        <b>Key Features:</b>
        • Secure API gateway for AS400 database access
        • Authentication and authorization controls
        • Rate limiting and circuit breaker patterns
        • Comprehensive audit logging
        • Health monitoring and auto-recovery
        • Input sanitization and SQL injection protection
        """
        
        self.story.append(Paragraph(overview_text, self.styles['Normal']))
        
        # Architecture diagram (text representation)
        self.story.append(Spacer(1, 20))
        self.story.append(Paragraph("System Architecture", self.heading_style))
        
        arch_text = """
        <font name="Courier" size="10">
        Azure MCP Server (10.200.0.1:8000)
                    ↓ VPN Tunnel
        Service Gateway (192.168.1.42:8080)
                    ↓ ODBC Connection
        AS400 Database (Internal Network)
        </font>
        """
        
        self.story.append(Paragraph(arch_text, self.styles['Normal']))
        self.story.append(PageBreak())
    
    def add_installation_guide(self):
        self.story.append(Paragraph("2. Installation Guide", self.title_style))
        
        # Prerequisites
        self.story.append(Paragraph("Prerequisites", self.heading_style))
        prereq_data = [
            ['Component', 'Version', 'Purpose'],
            ['Python', '3.11+', 'Runtime environment'],
            ['FastAPI', '0.104.1+', 'Web framework'],
            ['IBM i Access ODBC Driver', 'Latest', 'Database connectivity'],
            ['Windows Server', '2016+', 'Host operating system'],
            ['PowerShell', '5.1+', 'Service management']
        ]
        
        prereq_table = Table(prereq_data, colWidths=[2*inch, 1.5*inch, 2.5*inch])
        prereq_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.blackColor),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        self.story.append(prereq_table)
        
        # Installation steps
        install_text = """
        <b>Installation Steps:</b><br/>
        1. Create project directory: C:\\service-gateway<br/>
        2. Set up Python virtual environment<br/>
        3. Install dependencies from requirements.txt<br/>
        4. Configure environment variables in .env file<br/>
        5. Test database connectivity<br/>
        6. Install as Windows service using NSSM<br/>
        7. Configure automatic startup and monitoring<br/>
        8. Verify installation with health checks
        """
        
        self.story.append(Spacer(1, 20))
        self.story.append(Paragraph(install_text, self.styles['Normal']))
        self.story.append(PageBreak())
    
    def add_api_reference(self):
        self.story.append(Paragraph("3. API Reference", self.title_style))
        
        # Endpoints table
        endpoints_data = [
            ['Endpoint', 'Method', 'Purpose', 'Auth Required'],
            ['/health', 'GET', 'Basic health check', 'No'],
            ['/health/comprehensive', 'GET', 'Detailed health metrics', 'No'],
            ['/health/database', 'GET', 'Database connectivity check', 'No'],
            ['/api/customer', 'POST', 'Get customer by ID', 'Yes'],
            ['/api/customer/search', 'POST', 'Search customers', 'Yes'],
            ['/performance', 'GET', 'Performance statistics', 'No'],
            ['/audit/stats', 'GET', 'Audit log statistics', 'No']
        ]
        
        endpoints_table = Table(endpoints_data, colWidths=[2*inch, 0.8*inch, 2.2*inch, 1*inch])
        endpoints_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.blackColor),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        self.story.append(endpoints_table)
        
        # Authentication
        self.story.append(Spacer(1, 20))
        self.story.append(Paragraph("Authentication", self.heading_style))
        
        auth_text = """
        All protected endpoints require Bearer token authentication:
        
        Header: Authorization: Bearer [SHA256_TOKEN]
        
        Generate token: SHA256(API_SECRET_KEY from .env file)
        """
        
        self.story.append(Paragraph(auth_text, self.styles['Normal']))
        self.story.append(PageBreak())
    
    def add_maintenance_guide(self):
        self.story.append(Paragraph("4. Maintenance Guide", self.title_style))
        
        # Daily tasks
        self.story.append(Paragraph("Daily Tasks", self.heading_style))
        daily_text = """
        • Check service status: Get-Service ServiceGateway
        • Verify API health: curl http://localhost:8080/health
        • Review error logs for issues
        • Monitor disk space and memory usage
        """
        
        self.story.append(Paragraph(daily_text, self.styles['Normal']))
        
        # Weekly tasks  
        self.story.append(Paragraph("Weekly Tasks", self.heading_style))
        weekly_text = """
        • Review performance metrics
        • Check database connection stability
        • Analyze audit logs for security issues
        • Verify automatic log rotation
        """
        
        self.story.append(Paragraph(weekly_text, self.styles['Normal']))
        
        # Monthly tasks
        self.story.append(Paragraph("Monthly Tasks", self.heading_style))
        monthly_text = """
        • Full system health assessment
        • Security audit and review
        • Performance optimization review
        • Update dependencies (if needed)
        • Backup configuration files
        """
        
        self.story.append(Paragraph(monthly_text, self.styles['Normal']))
        self.story.append(PageBreak())
    
    def add_troubleshooting(self):
        self.story.append(Paragraph("5. Troubleshooting", self.title_style))
        
        # Common issues table
        issues_data = [
            ['Issue', 'Cause', 'Solution'],
            ['Service won\'t start', 'Config error or dependency issue', 'Check logs and restart service'],
            ['Database connection failed', 'AS400 unavailable or credentials', 'Verify connectivity and credentials'],
            ['High memory usage', 'Memory leak or heavy load', 'Restart service, monitor patterns'],
            ['Authentication failures', 'Invalid token or expired', 'Regenerate and update tokens'],
            ['Rate limit exceeded', 'Too many requests from client', 'Implement client-side throttling']
        ]
        
        issues_table = Table(issues_data, colWidths=[2*inch, 2*inch, 2*inch])
        issues_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.blackColor),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        self.story.append(issues_table)
        
        # Emergency contacts
        self.story.append(Spacer(1, 20))
        self.story.append(Paragraph("Emergency Contacts", self.heading_style))
        
        contacts_text = """
        <b>Primary Support:</b> IT Operations Team
        <b>Database Issues:</b> AS400 Administrator
        <b>Network Issues:</b> Network Operations Center
        <b>After Hours:</b> On-call rotation
        """
        
        self.story.append(Paragraph(contacts_text, self.styles['Normal']))
        self.story.append(PageBreak())

def generate_pdf():
    """Generate the complete PDF documentation"""
    try:
        pdf = ServiceGatewayPDF()
        pdf.generate_complete_documentation()
        return True
    except Exception as e:
        print(f"❌ PDF generation failed: {e}")
        return False

if __name__ == "__main__":
    generate_pdf()