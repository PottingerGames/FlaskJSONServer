from flask import Flask, request, jsonify
from datetime import datetime
import json

app = Flask(__name__)

API_KEY = "e7f13ab2-85a7-4d92-80c3-bf04cd519be3" 


def log_to_file(filename, data):
    """Helper function to append JSON to a .jsonl file."""
    data['received_at'] = datetime.utcnow().isoformat()
    with open(filename, "a") as f:
        f.write(json.dumps(data) + "\n")


@app.route('/receive-item', methods=['POST'])  # For Item Events
def receive_item():
    api_key = request.headers.get('x-api-key')
    if api_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    if request.is_json:
        data = request.get_json()
        log_to_file("logs_item.jsonl", data)
        return jsonify({"status": "item event logged"}), 200
    else:
        return jsonify({"error": "Request must be JSON"}), 400


@app.route('/receive-stage', methods=['POST'])  # For Stage Events
def receive_stage():
    api_key = request.headers.get('x-api-key')
    if api_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    if request.is_json:
        data = request.get_json()
        log_to_file("logs_stage.jsonl", data)
        return jsonify({"status": "stage event logged"}), 200
    else:
        return jsonify({"error": "Request must be JSON"}), 400


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

