from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/receive-json', methods=['POST'])
def receive_json():
    if request.is_json:
        data = request.get_json()
        # You can process data here
        print("Received JSON:", data)

        # Respond back with something
        return jsonify({"status": "success", "received": data}), 200
    else:
        return jsonify({"error": "Request must be JSON"}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
