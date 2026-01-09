import logging
import os
from datetime import datetime

class LoggerConfig:
    """Centralized logging configuration"""
    
    @staticmethod
    def setup_logger(name: str, log_dir: str = 'logs') -> logging.Logger:
        """
        Setup logger met file en console handlers
        
        :param name: Logger naam (meestal __name__ van module)
        :param log_dir: Directory voor log files
        :return: Configured logger
        """
        # Maak log directory
        os.makedirs(log_dir, exist_ok=True)
        
        # Logger instance
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)  # Capture everything
        
        # Voorkom duplicate handlers
        if logger.handlers:
            return logger
        
        # Timestamp voor log files
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # File handler - DEBUG level (alles)
        file_handler = logging.FileHandler(
            f'{log_dir}/scraper_{timestamp}.log',
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        
        # Console handler - INFO level (belangrijke info)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        
        # Error file handler - alleen errors
        error_handler = logging.FileHandler(
            f'{log_dir}/errors_{timestamp}.log',
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)
        
        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.addHandler(error_handler)
        
        logger.info(f"Logger '{name}' initialized")
        logger.debug(f"Log files created in: {log_dir}/")
        
        return logger