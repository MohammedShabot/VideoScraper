import pandas as pd
import itertools
from typing import List, Tuple, Dict
import os
from config import LoggerConfig

class QueryGenerator:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.df = None
        self.queries = []
        self.logger = LoggerConfig.setup_logger(__name__)
        
        self.logger.info("="*60)
        self.logger.info("QueryGenerator initialized")
        self.logger.info(f"CSV path: {csv_path}")
        self.logger.info("="*60)
    
    def load_csv(self) -> bool:
        """
        Laad CSV en toon info
        
        :return: True if successful, False otherwise
        """
        self.logger.info(f"Attempting to load CSV: {self.csv_path}")
        
        # Check if file exists
        if not os.path.exists(self.csv_path):
            self.logger.error(f"CSV file not found: {self.csv_path}")
            return False
        
        # Check file size
        file_size = os.path.getsize(self.csv_path)
        self.logger.debug(f"CSV file size: {file_size} bytes ({file_size/1024:.2f} KB)")
        
        try:
            # Load CSV
            self.logger.debug("Reading CSV with pandas...")
            self.df = pd.read_csv(self.csv_path, encoding='utf-8')
            
            # Log basic info
            self.logger.info(f"✅ CSV loaded successfully")
            self.logger.info(f"   Rows: {len(self.df)}")
            self.logger.info(f"   Columns: {list(self.df.columns)}")
            self.logger.debug(f"   Memory usage: {self.df.memory_usage(deep=True).sum() / 1024:.2f} KB")
            
            # Check for required columns
            required_cols = ['Emotion', 'Subject', 'Setting']
            missing_cols = [col for col in required_cols if col not in self.df.columns]
            
            if missing_cols:
                self.logger.error(f"Missing required columns: {missing_cols}")
                return False
            
            self.logger.debug("All required columns present")
            
            # Log data quality
            self.logger.debug("Checking data quality...")
            for col in required_cols:
                null_count = self.df[col].isnull().sum()
                unique_count = self.df[col].nunique()
                self.logger.debug(f"   {col}: {unique_count} unique values, {null_count} null values")
            
            # Show first few rows
            self.logger.debug("First 3 rows of data:")
            for idx, row in self.df.head(3).iterrows():
                self.logger.debug(f"   Row {idx}: {dict(row)}")
            
            return True
            
        except pd.errors.EmptyDataError:
            self.logger.error("CSV file is empty")
            return False
        except pd.errors.ParserError as e:
            self.logger.error(f"CSV parsing error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error loading CSV: {e}", exc_info=True)
            return False
    
    def generate_combinations(self) -> List[Tuple[str, str, str]]:
        """Genereer alle combinaties"""
        self.logger.info("Starting combination generation...")
        
        if self.df is None:
            self.logger.error("DataFrame is None - CSV not loaded")
            return []
        
        try:
            # Extract unique values
            self.logger.debug("Extracting unique values from columns...")
            
            emotions = self.df['Emotion'].dropna().unique().tolist()
            self.logger.info(f"   Extracted {len(emotions)} unique emotions")
            self.logger.debug(f"   Emotions: {emotions[:5]}..." if len(emotions) > 5 else f"   Emotions: {emotions}")
            
            subjects = self.df['Subject'].dropna().unique().tolist()
            self.logger.info(f"   Extracted {len(subjects)} unique subjects")
            self.logger.debug(f"   Subjects: {subjects[:5]}..." if len(subjects) > 5 else f"   Subjects: {subjects}")
            
            settings = self.df['Setting'].dropna().unique().tolist()
            self.logger.info(f"   Extracted {len(settings)} unique settings")
            self.logger.debug(f"   Settings: {settings[:5]}..." if len(settings) > 5 else f"   Settings: {settings}")
            
            # Calculate total combinations
            total_combinations = len(emotions) * len(subjects) * len(settings)
            self.logger.info(f"📊 Total possible combinations: {total_combinations:,}")
            
            # Generate combinations
            self.logger.debug("Generating Cartesian product...")
            combinations = list(itertools.product(emotions, subjects, settings))
            
            self.logger.info(f"✅ Generated {len(combinations):,} combinations")
            self.logger.debug(f"First 3 combinations: {combinations[:3]}")
            
            return combinations
            
        except KeyError as e:
            self.logger.error(f"Column not found: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error generating combinations: {e}", exc_info=True)
            return []
    
    def format_queries(self, combinations: List[Tuple], style: str = 'simple') -> List[Dict]:
        """Format combinaties als search queries"""
        self.logger.info(f"Formatting queries with style: '{style}'")
        
        if not combinations:
            self.logger.warning("No combinations to format")
            return []
        
        queries = []
        
        try:
            for idx, (emotion, subject, setting) in enumerate(combinations):
                if style == 'simple':
                    query_text = f"{emotion} {subject} {setting}"
                elif style == 'natural':
                    query_text = f"{subject} expressing {emotion} in {setting}"
                elif style == 'video':
                    query_text = f"{emotion} {subject} {setting} video"
                else:
                    self.logger.warning(f"Unknown style '{style}', using 'simple'")
                    query_text = f"{emotion} {subject} {setting}"
                
                query_obj = {
                    'id': idx,
                    'query': query_text,
                    'emotion': emotion,
                    'subject': subject,
                    'setting': setting,
                    'scraped': False
                }
                
                queries.append(query_obj)
                
                # Log progress every 1000 queries
                if (idx + 1) % 1000 == 0:
                    self.logger.debug(f"   Formatted {idx + 1:,}/{len(combinations):,} queries")
            
            self.logger.info(f"✅ Formatted {len(queries):,} queries")
            self.logger.debug(f"Sample query: {queries[0]}")
            
            return queries
            
        except Exception as e:
            self.logger.error(f"Error formatting queries: {e}", exc_info=True)
            return []
    
    def generate_all_queries(self, style: str = 'simple') -> List[Dict]:
        """Complete query generation pipeline"""
        self.logger.info("="*60)
        self.logger.info("STARTING QUERY GENERATION PIPELINE")
        self.logger.info("="*60)
        
        # Step 1: Load CSV
        if not self.load_csv():
            self.logger.error("Failed to load CSV - aborting")
            return []
        
        # Step 2: Generate combinations
        combinations = self.generate_combinations()
        if not combinations:
            self.logger.error("No combinations generated - aborting")
            return []
        
        # Step 3: Format queries
        self.queries = self.format_queries(combinations, style)
        if not self.queries:
            self.logger.error("No queries formatted - aborting")
            return []
        
        self.logger.info("="*60)
        self.logger.info("QUERY GENERATION COMPLETE")
        self.logger.info(f"Total queries: {len(self.queries):,}")
        self.logger.info("="*60)
        
        # Show sample queries
        self.logger.info("Sample queries (first 5):")
        for q in self.queries[:5]:
            self.logger.info(f"   {q['id']}: {q['query']}")
        
        return self.queries