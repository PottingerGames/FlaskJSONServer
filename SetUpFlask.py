from flask import Flask, request, jsonify
from datetime import datetime
import json

app = Flask(__name__)

API_KEY = "e7f13ab2-85a7-4d92-80c3-bf04cd519be3"  # Replace this with your real key

@app.route('/receive-json', methods=['POST'])
def receive_json():
    # Check API key
    api_key = request.headers.get('x-api-key')
    if api_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    # Validate and save JSON
    if request.is_json:
        data = request.get_json()
        data['received_at'] = datetime.utcnow().isoformat()  # Add timestamp

        # Append to log file (newline-delimited JSON)
        with open("logs.jsonl", "a") as f:
            f.write(json.dumps(data) + "\n")

        return jsonify({"status": "logged"}), 200
    else:
        return jsonify({"error": "Request must be JSON"}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

