from flask import Flask, request, jsonify
from datetime import datetime
import json
import os

app = Flask(__name__)

API_KEY = "e7f13ab2-85a7-4d92-80c3-bf04cd519be3"

# Resolve base directory relative to this file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def log_to_file(filename, data):
    data['received_at'] = datetime.utcnow().isoformat()
    file_path = os.path.join(BASE_DIR, filename)
    with open(file_path, "a") as f:
        f.write(json.dumps(data) + "\n")

@app.route('/receive-item', methods=['POST'])
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

@app.route('/receive-stage', methods=['POST'])
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

@app.route('/get-item-logs', methods=['GET'])
def get_item_logs():
    api_key = request.headers.get('x-api-key')
    if api_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    try:
        file_path = os.path.join(BASE_DIR, "logs_item.jsonl")
        with open(file_path, "r") as f:
            lines = f.readlines()
            return jsonify([json.loads(line) for line in lines])
    except FileNotFoundError:
        return jsonify([])

@app.route('/get-stage-logs', methods=['GET'])
def get_stage_logs():
    api_key = request.headers.get('x-api-key')
    if api_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    try:
        file_path = os.path.join(BASE_DIR, "logs_stage.jsonl")
        with open(file_path, "r") as f:
            lines = f.readlines()
            return jsonify([json.loads(line) for line in lines])
    except FileNotFoundError:
        return jsonify([])
    
@app.route('/receive-error', methods=['POST'])
def receive_error():
    api_key = request.headers.get('x-api-key')
    if api_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    if request.is_json:
        data = request.get_json()
        log_to_file("logs_error.jsonl", data) # Log to a new error log file
        return jsonify({"status": "error event logged"}), 200
    else:
        return jsonify({"error": "Request must be JSON"}), 400
    
@app.route('/get-error-logs', methods=['GET'])
def get_error_logs():
    api_key = request.headers.get('x-api-key')
    if api_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    try:
        file_path = os.path.join(BASE_DIR, "logs_error.jsonl")
        with open(file_path, "r") as f:
            lines = f.readlines()
            return jsonify([json.loads(line) for line in lines])
    except FileNotFoundError:
        return jsonify([])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

