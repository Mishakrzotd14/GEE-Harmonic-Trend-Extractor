import os

import google.oauth2.credentials
import google_auth_oauthlib.flow


def get_google_drive_credentials(token_json, credentials_json, SCOPES):
    """Получает учетные данные Google Drive."""
    creds = None
    if os.path.exists(token_json):
        creds = google.oauth2.credentials.Credentials.from_authorized_user_file(token_json, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(google.auth.transport.requests.Request())
        else:
            flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(credentials_json, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_json, "w") as token:
            token.write(creds.to_json())
    return creds
