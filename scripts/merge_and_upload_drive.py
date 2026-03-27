import os
import time
import argparse
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Load .env file if it exists (for local dev)
load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/presentations']

def get_creds():
    """Builds credentials and validates environment variables."""
    client_id = os.environ.get("GOOGLE_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET")
    refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        print("\n❌ ERROR: Missing Environment Variables!")
        print(f"DEBUG: CLIENT_ID: {'Set' if client_id else 'MISSING'}")
        print(f"DEBUG: CLIENT_SECRET: {'Set' if client_secret else 'MISSING'}")
        print(f"DEBUG: REFRESH_TOKEN: {'Set' if refresh_token else 'MISSING'}\n")
        raise SystemExit("Please set your variables in a .env file or Railway dashboard.")

    return Credentials(
        token=None,
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        token_uri="https://oauth2.googleapis.com/token"
    )

def upload_and_convert(drive_service, file_path):
    """Uploads PPTX and converts it to Google Slides format."""
    file_metadata = {
        'name': f"TEMP_{Path(file_path).stem}",
        'mimeType': 'application/vnd.google-apps.presentation' 
    }
    media = MediaFileUpload(
        file_path, 
        mimetype='application/vnd.openxmlformats-officedocument.presentationml.presentation',
        resumable=True
    )
    
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return file.get('id')

def merge_slides(slides_service, base_id, append_ids):
    """Copies slides from append_ids into base_id using copyPasteSlide."""
    for presentation_id in append_ids:
        # 1. Get slide IDs from the source presentation
        src_prs = slides_service.presentations().get(presentationId=presentation_id).execute()
        slides = src_prs.get('slides', [])
        
        if not slides:
            continue

        # 2. Prepare requests for this specific file
        # We process one file at a time to avoid huge payload timeouts
        requests = []
        for slide in slides:
            requests.append({
                'copyPasteSlide': {
                    'sourceSlideId': slide['objectId'],
                    'destinationPresentationId': base_id,
                    'linkingMode': 'NOT_LINKED'
                }
            })
        
        print(f"  -> Copying {len(requests)} slides from {presentation_id}...")
        slides_service.presentations().batchUpdate(presentationId=base_id, body={'requests': requests}).execute()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ppt', action='append', required=True)
    parser.add_argument('--name', default="Merged Presentation")
    args = parser.parse_args()

    creds = get_creds()
    drive_service = build('drive', 'v3', credentials=creds)
    slides_service = build('slides', 'v1', credentials=creds)

    uploaded_ids = []
    try:
        # 1. Upload and Convert all files
        for path in args.ppt:
            abs_path = str(Path(path).resolve())
            print(f"Step 1: Uploading & Converting: {Path(path).name}")
            file_id = upload_and_convert(drive_service, abs_path)
            uploaded_ids.append(file_id)

        # 2. Setup the Merge
        base_id = uploaded_ids[0]
        to_append = uploaded_ids[1:]

        # 3. Rename the first file to your desired final name
        drive_service.files().update(fileId=base_id, body={'name': args.name}).execute()

        # 4. Merge others into the base
        print(f"Step 2: Merging into '{args.name}'...")
        merge_slides(slides_service, base_id, to_append)
        
        print(f"\n✅ SUCCESS!")
        print(f"Merged File ID: {base_id}")
        print(f"URL: https://docs.google.com/presentation/d/{base_id}/edit")

    except Exception as e:
        print(f"\n❌ FAILED: {str(e)}")
    finally:
        # 5. Cleanup: Delete the source converted files (except the base/merged one)
        if len(uploaded_ids) > 1:
            print("Step 3: Cleaning up temporary files...")
            for fid in uploaded_ids[1:]:
                try:
                    drive_service.files().delete(fileId=fid).execute()
                except:
                    pass

if __name__ == "__main__":
    main()