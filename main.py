import json
import requests
import csv
from datetime import datetime, timedelta
import time
import pandas as pd
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

class GenesysIDFinder:
    def __init__(self, client_id, client_secret, environment="mypurecloud.com"):
        self.base_url = f"https://api.{environment}"
        self.login_url = f"https://login.{environment}"
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.headers = None
        self.environment = environment
        self.authenticate()

    def authenticate(self):
        try:
            # Add more robust authentication with retry mechanism
            token_url = f"{self.login_url}/oauth/token"
            data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret
            }
            
            # Implement exponential backoff for authentication
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = requests.post(token_url, data=data)
                    response.raise_for_status()
                    token_data = response.json()
                    self.access_token = token_data['access_token']
                    self.headers = {
                        'Authorization': f'Bearer {self.access_token}',
                        'Content-Type': 'application/json'
                    }
                    logging.info("Successfully authenticated with Genesys Cloud")
                    return
                except requests.exceptions.RequestException as e:
                    logging.warning(f"Authentication attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        # Exponential backoff
                        time.sleep(2 ** attempt)
                    else:
                        raise
        except Exception as e:
            logging.error(f"Authentication failed: {e}")
            raise

    def find_ids(self, start_date, end_date, output_file="recording_ids.csv"):
        """
        Find recording IDs and call IDs for a specific time range
        
        Args:
            start_date (str): Start date in format 'YYYY-MM-DD'
            end_date (str): End date in format 'YYYY-MM-DD'
            output_file (str): Name of the CSV file to save results
        """
        # Convert dates to datetime objects
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)  # Include the entire end date

        # Print detailed debugging information
        print(f"Base URL: {self.base_url}")
        print(f"Access Token: {self.access_token[:10]}...")  # Partial token for security
        
        recordings_url = f"{self.base_url}/api/v2/recordings/query"
        print(f"Recordings Query URL: {recordings_url}")
        
        body = {
            "interval": f"{start_dt.isoformat()}Z/{end_dt.isoformat()}Z",
            "order": "desc",
            "pageSize": 100
        }
        print(f"Query Body: {body}")

        # Prepare CSV file
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Conversation ID', 'Recording ID', 'Start Time', 'Duration (seconds)', 'Agent Name'])

            page_count = 1
            total_recordings = 0
            
            while True:
                print(f"Fetching page {page_count}...")
                try:
                    response = requests.post(recordings_url, headers=self.headers, json=body)
                    
                    # Log full response details for debugging
                    print(f"Response Status Code: {response.status_code}")
                    print(f"Response Headers: {response.headers}")
                    print(f"Response Text: {response.text}")
                    
                    # Raise an exception for non-200 status codes
                    response.raise_for_status()
                    
                    data = response.json()
                    conversations = data.get('conversations', [])
                    
                    for conv in conversations:
                        conv_id = conv.get('conversationId')
                        # Get recordings for this conversation
                        recordings_detail_url = f"{self.base_url}/api/v2/conversations/{conv_id}/recordings"
                        rec_response = requests.get(recordings_detail_url, headers=self.headers)
                        
                        if rec_response.status_code == 200:
                            recordings = rec_response.json()
                            for recording in recordings:
                                writer.writerow([
                                    conv_id,
                                    recording.get('id', 'N/A'),
                                    recording.get('startTime', 'N/A'),
                                    recording.get('durationMilliseconds', 0) / 1000,
                                    recording.get('agent', {}).get('name', 'N/A')
                                ])
                                total_recordings += 1
                        else:
                            print(f"Failed to get recordings for conversation {conv_id}: {rec_response.text}")
                        
                        # Respect API rate limits
                        time.sleep(0.2)
                    
                    if not data.get('nextUri'):
                        break
                        
                    body['pageNumber'] = page_count
                    page_count += 1
                
                except requests.exceptions.RequestException as e:
                    print(f"Request failed: {str(e)}")
                    break
                except Exception as e:
                    print(f"Unexpected error: {str(e)}")
                    break

        print(f"\nCompleted! Found {total_recordings} recordings.")
        print(f"Results saved to {output_file}")

    def find_recordings_by_conversation_id(self, conversation_id, output_file="recording_ids.csv"):
        try:
            # Re-authenticate if token is likely expired
            if not self.access_token:
                self.authenticate()
            
            # Add rate limiting and error handling
            recordings = []
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    url = f"{self.base_url}/api/v2/conversations/{conversation_id}/recordings"
                    response = requests.get(url, headers=self.headers)
                    response.raise_for_status()
                    
                    recordings = response.json().get('entities', [])
                    
                    # Pause between requests to avoid rate limiting
                    time.sleep(0.5)
                    
                    break
                except requests.exceptions.RequestException as e:
                    logging.warning(f"Attempt {attempt + 1} failed to retrieve recordings for {conversation_id}: {e}")
                    if attempt < max_retries - 1:
                        # Re-authenticate and retry
                        self.authenticate()
                        time.sleep(2 ** attempt)
                    else:
                        logging.error(f"Failed to retrieve recordings after {max_retries} attempts")
                        return []
            
            # Save to CSV if recordings found
            if recordings:
                df = pd.DataFrame(recordings)
                df.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False)
            
            return recordings
        
        except Exception as e:
            logging.error(f"Error in find_recordings_by_conversation_id: {e}")
            return []

    def find_recordings_by_conversation_id(self, conversation_id, output_file="recording_ids.csv"):
        try:
            # Re-authenticate if token is likely expired
            if not self.access_token:
                self.authenticate()
            
            # Add rate limiting and error handling
            recordings = []
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    url = f"{self.base_url}/api/v2/conversations/{conversation_id}/recordings"
                    response = requests.get(url, headers=self.headers)
                    response.raise_for_status()
                    
                    # Try different ways of extracting recordings
                    response_json = response.json()
                    recordings = response_json.get('entities', response_json) if isinstance(response_json, dict) else response_json
                    
                    # Ensure recordings is a list
                    if not isinstance(recordings, list):
                        recordings = [recordings]
                    
                    # Determine if we need to write headers
                    file_exists = os.path.exists(output_file)
                    file_is_empty = not file_exists or os.path.getsize(output_file) == 0
                    
                    # Open the file in append mode
                    with open(output_file, 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        
                        # Write headers only if the file is new or empty
                        if file_is_empty:
                            writer.writerow(['Conversation ID', 'Recording ID', 'Start Time', 'End Time', 'Media Type', 'File State', 'Download URL'])
                        
                        # Write recording details
                        for recording in recordings:
                            writer.writerow([
                                conversation_id,
                                recording.get('id', 'N/A'),
                                recording.get('startTime', 'N/A'),
                                recording.get('endTime', 'N/A'),
                                recording.get('media', recording.get('mediaType', 'N/A')),
                                recording.get('fileState', 'N/A'),
                                recording.get('mediaUris', {}).get('0', {}).get('mediaUri', 
                                    recording.get('download', {}).get('url', 'N/A'))
                            ])
                        
                        print(f"Added {len(recordings)} recordings for conversation {conversation_id} to {output_file}")
                    
                    break
                except requests.exceptions.RequestException as e:
                    logging.warning(f"Attempt {attempt + 1} failed to retrieve recordings for {conversation_id}: {e}")
                    if attempt < max_retries - 1:
                        # Re-authenticate and retry
                        self.authenticate()
                        time.sleep(2 ** attempt)
                    else:
                        logging.error(f"Failed to retrieve recordings after {max_retries} attempts")
                        return []
            
            return recordings
        
        except Exception as e:
            logging.error(f"Error retrieving recordings for conversation {conversation_id}: {str(e)}")
            return []

    def get_conversation_details(self, conversation_id):
        """
        Retrieve conversation details with improved error handling.
        
        Args:
            conversation_id (str): The ID of the conversation to retrieve.
        
        Returns:
            dict or None: Conversation details if available, None otherwise.
        """
        try:
            url = f"{self.base_url}/api/v2/conversations/{conversation_id}"
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 403:
                print(f"Warning: Not authorized to access conversation details. Status: {response.status_code}")
                print(f"Response Text: {response.text}")
                return None
            else:
                print(f"Unexpected status code when retrieving conversation details: {response.status_code}")
                print(f"Response Text: {response.text}")
                return None
        
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving conversation details: {e}")
            return None

    def check_recording_status(self, conversation_id, recording_id):
        """
        Check the status of a specific recording and provide detailed information.
        
        Args:
            conversation_id (str): The ID of the conversation
            recording_id (str): The ID of the recording
        
        Returns:
            dict: Detailed status information about the recording
        """
        try:
            # Construct the URL for checking recording status
            status_url = f"{self.base_url}/api/v2/conversations/{conversation_id}/recordings/{recording_id}"
            
            # Prepare headers with access token
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            # Send GET request to fetch recording status
            response = requests.get(status_url, headers=headers)
            
            # Log full response details for debugging
            print(f"Recording Status URL: {status_url}")
            print(f"Response Status Code: {response.status_code}")
            print(f"Response Headers: {response.headers}")
            
            try:
                response_text = response.text
                print(f"Response Text: {response_text}")
                
                # Try to parse JSON, handle potential decoding errors
                if response_text:
                    response_json = json.loads(response_text)
                    print("\nRecording Status Details:")
                    print(json.dumps(response_json, indent=2))
                    return response_json
                else:
                    print("Empty response text")
                    return {}
            
            except json.JSONDecodeError as json_err:
                print(f"JSON Decoding Error: {json_err}")
                print(f"Raw Response Text: {response_text}")
                return {}
        
        except requests.exceptions.RequestException as e:
            print(f"Error checking recording status: {str(e)}")
            return {}

    def read_conversation_ids(self, file_path):
        """
        Read conversation IDs from a CSV or Excel file with robust handling.
        
        Args:
            file_path (str): Path to the CSV or Excel file
        
        Returns:
            list: List of conversation IDs
        """
        # List of encodings to try
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        
        # List of possible delimiters
        delimiters = [',', ';', '\t', '|']
        
        # Common variations of column names for Recording ID
        id_column_variations = ['conversation_id', 'Call ID', 'call_id', 'CallID', 'ID', 'id', 'Recording ID', 'recording_id', 'RecordingID']
        
        # Determine file type based on extension
        if file_path.endswith('.csv'):
            # Try different encodings and delimiters
            for encoding in encodings:
                for delimiter in delimiters:
                    try:
                        # Add error_bad_lines and warn_bad_lines for more robust parsing
                        df = pd.read_csv(
                            file_path, 
                            encoding=encoding, 
                            sep=delimiter, 
                            on_bad_lines='warn'  # Skip bad lines instead of raising an error
                        )
                        
                        print(f"Successfully read CSV with {encoding} encoding and '{delimiter}' delimiter")
                        print("Available columns:", list(df.columns))
                        
                        # Break out of both loops if successful
                        break
                    except Exception as e:
                        print(f"Failed to read CSV with {encoding} encoding and '{delimiter}' delimiter: {e}")
                        continue
                else:
                    continue
                
                # If we successfully read the file, break the encoding loop
                break
            else:
                # If no encoding and delimiter combination works, raise an error
                raise ValueError(f"Could not read the CSV file with encodings {encodings} and delimiters {delimiters}")
        
        elif file_path.endswith(('.xls', '.xlsx')):
            # For Excel files, try different encodings if needed
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Unsupported file type. Please use CSV or Excel files.")
        
        # Print column names for debugging
        print("Final columns:", list(df.columns))
        
        # Find the first matching column
        id_column = next((col for col in id_column_variations if col in df.columns), None)
        
        if not id_column:
            raise ValueError(f"Could not find a Call/Recording ID column. Available columns: {list(df.columns)}")
        
        # Extract conversation IDs
        conversation_ids = df[id_column].tolist()
        
        return conversation_ids

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        filename='genesys_retrieval.log')
    
    try:
        # Read credentials from .env file
        CLIENT_ID = os.getenv('GENESYS_CLIENT_ID')
        CLIENT_SECRET = os.getenv('GENESYS_CLIENT_SECRET')
        ENVIRONMENT = os.getenv('GENESYS_ENVIRONMENT', 'mypurecloud.com')
        
        # Validate credentials
        if not CLIENT_ID or not CLIENT_SECRET:
            raise ValueError("Missing Genesys Cloud credentials. Please check your .env file.")
        
        # Initialize finder
        finder = GenesysIDFinder(CLIENT_ID, CLIENT_SECRET, ENVIRONMENT)
        
        # Read conversation IDs
        conversation_ids = finder.read_conversation_ids('call_ids.csv')
        
        # Limit to first 800 rows with progress tracking
        conversation_ids = conversation_ids[:800]
        
        # Create a list to store results for all conversations
        all_conversation_results = []
        
        # Process conversations with progress tracking
        for i, conversation_id in enumerate(conversation_ids, 1):
            logging.info(f"Processing conversation {i}/{len(conversation_ids)}: {conversation_id}")
            
            try:
                recordings = finder.find_recordings_by_conversation_id(conversation_id)
                if recordings:
                    all_conversation_results.extend(recordings)
                
                # Additional rate limiting
                if i % 50 == 0:
                    logging.info(f"Pausing to avoid rate limits...")
                    time.sleep(5)
            
            except Exception as e:
                logging.error(f"Error processing conversation {conversation_id}: {e}")
                continue
        
        logging.info(f"Processed {len(all_conversation_results)} recordings in total")
    
    except Exception as e:
        logging.error(f"Critical error in main process: {e}")

if __name__ == "__main__":
    main()
