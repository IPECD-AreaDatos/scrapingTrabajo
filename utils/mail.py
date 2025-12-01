"""
Utilidades para envío de correos electrónicos
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import Optional, List
import os
import logging
from pathlib import Path


class MailSender:
    """
    Clase para enviar correos electrónicos.
    Configuración mediante variables de entorno.
    """
    
    def __init__(
        self,
        smtp_server: Optional[str] = None,
        smtp_port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        from_email: Optional[str] = None
    ):
        """
        Inicializa el sender de correos.
        
        Args:
            smtp_server: Servidor SMTP (opcional, usa env si no se proporciona)
            smtp_port: Puerto SMTP (opcional)
            username: Usuario SMTP (opcional)
            password: Contraseña SMTP (opcional)
            from_email: Email remitente (opcional)
        """
        self.smtp_server = smtp_server or os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = smtp_port or int(os.getenv('SMTP_PORT', '587'))
        self.username = username or os.getenv('SMTP_USERNAME')
        self.password = password or os.getenv('SMTP_PASSWORD')
        self.from_email = from_email or os.getenv('SMTP_FROM_EMAIL')
        
        self.logger = logging.getLogger(__name__)
    
    def send_email(
        self,
        to_emails: List[str],
        subject: str,
        body: str,
        html_body: Optional[str] = None,
        attachments: Optional[List[Path]] = None
    ) -> bool:
        """
        Envía un correo electrónico.
        
        Args:
            to_emails: Lista de destinatarios
            subject: Asunto del correo
            body: Cuerpo del correo (texto plano)
            html_body: Cuerpo HTML (opcional)
            attachments: Lista de archivos a adjuntar (opcional)
            
        Returns:
            True si el envío fue exitoso
        """
        try:
            # Crear mensaje
            msg = MIMEMultipart('alternative')
            msg['From'] = self.from_email
            msg['To'] = ', '.join(to_emails)
            msg['Subject'] = subject
            
            # Agregar cuerpo
            if html_body:
                part1 = MIMEText(body, 'plain')
                part2 = MIMEText(html_body, 'html')
                msg.attach(part1)
                msg.attach(part2)
            else:
                msg.attach(MIMEText(body, 'plain'))
            
            # Agregar adjuntos
            if attachments:
                for file_path in attachments:
                    if file_path.exists():
                        with open(file_path, 'rb') as f:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(f.read())
                            encoders.encode_base64(part)
                            part.add_header(
                                'Content-Disposition',
                                f'attachment; filename= {file_path.name}'
                            )
                            msg.attach(part)
            
            # Enviar
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                if self.username and self.password:
                    server.login(self.username, self.password)
                server.send_message(msg)
            
            self.logger.info(f"Correo enviado exitosamente a {', '.join(to_emails)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al enviar correo: {e}")
            return False



