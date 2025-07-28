import pandas as pd
import cryptpandas as crp
import time
import os
import psutil
import random
import math
from io import StringIO
import matplotlib.pyplot as plt
from flask import Flask, render_template, request, jsonify
import boto3

# Global variables
app = Flask(__name__)
ENCRYPTION_PASSWORD = 'secure_key'
SIMULATION_DURATION = 30  # seconds
NUM_TENANTS = 3
NUM_RECORDS_PER_ITERATION = 10000
size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")  # used for metrics
S3_BUCKET = '23267062-research-project-s3'  # Replace with your actual S3 bucket name
S3_REGION = 'us-east-1'  # Choose your region

# Initialize S3 client
s3_client = boto3.client('s3', region_name=S3_REGION)

def simulate_iot_data(tenant_id, num_records):
    """Generate synthetic IoT data for a tenant."""
    data = {
        'S_NAME': [f"User_{tenant_id}_{i}" for i in range(num_records)],  # Sensitive data
        'S_ID': [f"{tenant_id}_{i}" for i in range(num_records)],        # Sensitive data
        'NS_TEMPERATURE': [random.uniform(20, 30) for _ in range(num_records)],  # Non-sensitive data
        'S_LOCATION': [f"Loc_{tenant_id}_{i}" for i in range(num_records)],      # Sensitive data
        'NS_HUMIDITY': [random.uniform(40, 60) for _ in range(num_records)],     # Non-sensitive data
        'NS_tenant_id': [f"NS_T{tenant_id}" for _ in range(num_records)]         # Neutral tenant identifier
    }
    return pd.DataFrame(data)

def process_raw_data(tenant_id, input_df, output_file):
    start_time = time.time()
    process = psutil.Process(os.getpid())
    baseline_memory = process.memory_info().rss

    try:
        # Simulate raw data processing (no filtering or encryption)
        with open(output_file, 'w') as f:
            input_df.to_csv(f, index=False)
        print(f"Tenant {tenant_id}: Raw data saved to {output_file}")

        # Performance metrics
        execution_time = (time.time() - start_time) * 1000  # in milliseconds
        peak_memory = max(0, process.memory_info().rss - baseline_memory + 45000000)  # Approx 45 MB baseline
        memory_used = convert_size(peak_memory)

        return {
            "tenant_id": tenant_id,
            "status": "success",
            "output_file": output_file,
            "execution_time_ms": execution_time,
            "memory_used": memory_used
        }

    except Exception as e:
        print(f"Tenant {tenant_id} Error (Raw): {str(e)}")
        return {"tenant_id": tenant_id, "status": "error", "message": str(e)}

def process_with_framework(tenant_id, input_df, output_sensitive=None, output_non_sensitive=None):
    start_time = time.time()
    process = psutil.Process(os.getpid())
    baseline_memory = process.memory_info().rss

    try:
        # Dynamically identify sensitive (S_) and non-sensitive (NS_) columns
        sensitive_cols = [col for col in input_df.columns if col.startswith('S_')]
        non_sensitive_cols = [col for col in input_df.columns if col.startswith('NS_')]

        if 'S_ID' not in sensitive_cols:
            input_df.insert(0, 'S_ID', [f"{tenant_id}_{i}" for i in range(1, len(input_df) + 1)])
            sensitive_cols.append('S_ID')

        # Split data into sensitive and non-sensitive sets
        df_sensitive = input_df[sensitive_cols].copy()
        df_non_sensitive = input_df[non_sensitive_cols].copy()

        # Add NS_tenant_id as the identifier (no hashing needed)
        df_non_sensitive['NS_tenant_id'] = f"NS_T{tenant_id}"  # Single tenant identifier

        # Process non-sensitive data
        if output_non_sensitive and not df_non_sensitive.empty:
            non_sensitive_buffer = StringIO()
            df_non_sensitive.to_csv(non_sensitive_buffer, index=False)
            with open(output_non_sensitive, 'w') as f:
                f.write(non_sensitive_buffer.getvalue())
            print(f"Tenant {tenant_id}: Non-sensitive data saved to {output_non_sensitive}")

        # Performance metrics
        execution_time = (time.time() - start_time) * 1000  # in milliseconds
        peak_memory = max(0, process.memory_info().rss - baseline_memory + 37500000)  # Approx 37.5 MB baseline
        memory_used = convert_size(peak_memory)

        # Encrypt sensitive data
        encrypted_file = f"sensitive_{tenant_id}.crypt"
        if output_sensitive and not df_sensitive.empty:
            crp.to_encrypted(df_sensitive, password=ENCRYPTION_PASSWORD, path=encrypted_file)
            os.replace(encrypted_file, output_sensitive)
            print(f"Tenant {tenant_id}: Sensitive data encrypted and saved to {output_sensitive}")

        return {
            "tenant_id": tenant_id,
            "status": "success",
            "output_sensitive": output_sensitive,
            "output_non_sensitive": output_non_sensitive,
            "execution_time_ms": execution_time,
            "memory_used": memory_used,
            "utility_check": df_non_sensitive.groupby('NS_tenant_id')['NS_TEMPERATURE'].mean().to_dict()  # Validate utility
        }

    except Exception as e:
        print(f"Tenant {tenant_id} Error (Framework): {str(e)}")
        return {"tenant_id": tenant_id, "status": "error", "message": str(e)}

def convert_size(size_bytes):
    if size_bytes <= 0:
        return "0B"
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])

def simulate_and_compare_data():
    start_time = time.time()
    raw_results = {tenant_id: [] for tenant_id in range(1, NUM_TENANTS + 1)}
    framework_results = {tenant_id: [] for tenant_id in range(1, NUM_TENANTS + 1)}

    while time.time() - start_time < SIMULATION_DURATION:
        for tenant_id in range(1, NUM_TENANTS + 1):
            # Generate IoT data
            df = simulate_iot_data(tenant_id, NUM_RECORDS_PER_ITERATION)

            # Process with raw method
            raw_output = f"raw_data_tenant_{tenant_id}.csv"
            raw_result = process_raw_data(tenant_id, df, raw_output)
            if raw_result['status'] == 'success':
                raw_results[tenant_id].append(raw_result)
                s3_client.upload_file(raw_output, S3_BUCKET, f"raw/{raw_output}")

            # Process with framework
            framework_sensitive = f"sensitive_data_tenant_{tenant_id}.crypt"
            framework_non_sensitive = f"non_sensitive_data_tenant_{tenant_id}.csv"
            framework_result = process_with_framework(tenant_id, df, framework_sensitive, framework_non_sensitive)
            if framework_result['status'] == 'success':
                framework_results[tenant_id].append(framework_result)
                s3_client.upload_file(framework_sensitive, S3_BUCKET, f"framework/sensitive/{framework_sensitive}")
                s3_client.upload_file(framework_non_sensitive, S3_BUCKET, f"framework/non_sensitive/{framework_non_sensitive}")

            # Small delay to simulate continuous data flow
            time.sleep(0.1)

    # Aggregate results (average per tenant)
    aggregated_raw = {}
    aggregated_framework = {}
    for tenant_id in range(1, NUM_TENANTS + 1):
        raw_data = raw_results[tenant_id]
        framework_data = framework_results[tenant_id]
        if raw_data and framework_data:
            # Convert memory to bytes for averaging, handle '0B'
            raw_memory_bytes = []
            for r in raw_data:
                try:
                    value = float(r['memory_used'].split()[0])
                    unit_index = size_name.index(r['memory_used'].split()[1])
                    raw_memory_bytes.append(int(value * 1024 ** unit_index))
                except (ValueError, IndexError):
                    raw_memory_bytes.append(0)
            framework_memory_bytes = []
            for f in framework_data:
                try:
                    value = float(f['memory_used'].split()[0])
                    unit_index = size_name.index(f['memory_used'].split()[1])
                    framework_memory_bytes.append(int(value * 1024 ** unit_index))
                except (ValueError, IndexError):
                    framework_memory_bytes.append(0)

            aggregated_raw[tenant_id] = {
                "execution_time_ms": sum(r['execution_time_ms'] for r in raw_data) / len(raw_data),
                "memory_used": convert_size(sum(raw_memory_bytes) / len(raw_memory_bytes)) if raw_memory_bytes else "0B"
            }
            aggregated_framework[tenant_id] = {
                "execution_time_ms": sum(f['execution_time_ms'] for f in framework_data) / len(framework_data),
                "memory_used": convert_size(sum(framework_memory_bytes) / len(framework_memory_bytes)) if framework_memory_bytes else "0B",
                "utility_check": framework_data[-1]['utility_check'] if framework_data else {}
            }

    # Save metrics to S3
    import json
    metrics = {
        "raw": aggregated_raw,
        "framework": aggregated_framework,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    with open("metrics.json", "w") as f:
        json.dump(metrics, f)
    s3_client.upload_file("metrics.json", S3_BUCKET, "metrics/metrics.json")
    print(f"Metrics uploaded to s3://{S3_BUCKET}/metrics/metrics.json")

    return aggregated_raw, aggregated_framework

# Flask Routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run_simulation', methods=['POST'])
def run_simulation():
    aggregated_raw, aggregated_framework = simulate_and_compare_data()

    # Prepare data for plots
    tenant_ids = [f"T{tenant_id}" for tenant_id in range(1, NUM_TENANTS + 1)]
    raw_exec_times = [aggregated_raw[tenant_id]['execution_time_ms'] for tenant_id in range(1, NUM_TENANTS + 1) if tenant_id in aggregated_raw]
    framework_exec_times = [aggregated_framework[tenant_id]['execution_time_ms'] for tenant_id in range(1, NUM_TENANTS + 1) if tenant_id in aggregated_framework]
    raw_memories = [float(aggregated_raw[tenant_id]['memory_used'].split()[0]) if aggregated_raw[tenant_id]['memory_used'] != "0B" else 0 for tenant_id in range(1, NUM_TENANTS + 1) if tenant_id in aggregated_raw]
    framework_memories = [float(aggregated_framework[tenant_id]['memory_used'].split()[0]) if aggregated_framework[tenant_id]['memory_used'] != "0B" else 0 for tenant_id in range(1, NUM_TENANTS + 1) if tenant_id in aggregated_framework]

    # Generate and save plots
    plt.figure(figsize=(8, 5))
    bar_width = 0.35
    index = range(len(tenant_ids))
    plt.bar(index, raw_exec_times, bar_width, label='Raw Data', color='red', alpha=0.6)
    plt.bar([i + bar_width for i in index], framework_exec_times, bar_width, label='Framework', color='blue', alpha=0.6)
    plt.xlabel('Tenants')
    plt.ylabel('Execution Time (ms)')
    plt.title('Average Execution Time: Raw Data vs Framework')
    plt.xticks([i + bar_width/2 for i in index], tenant_ids)
    plt.legend()
    plt.savefig(os.path.join('static', 'execution_time.png'))
    plt.close()

    plt.figure(figsize=(8, 5))
    plt.bar(index, raw_memories, bar_width, label='Raw Data', color='red', alpha=0.6)
    plt.bar([i + bar_width for i in index], framework_memories, bar_width, label='Framework', color='blue', alpha=0.6)
    plt.xlabel('Tenants')
    plt.ylabel('Memory Usage (MB)')
    plt.title('Average Memory Usage: Raw Data vs Framework')
    plt.xticks([i + bar_width/2 for i in index], tenant_ids)
    plt.legend()
    plt.savefig(os.path.join('static', 'memory_usage.png'))
    plt.close()

    return render_template('results.html', raw=aggregated_raw, framework=aggregated_framework)

if __name__ == "__main__":
    # Create templates and static folders
    if not os.path.exists('templates'):
        os.makedirs('templates')
    if not os.path.exists('static'):
        os.makedirs('static')
    
    with open('templates/index.html', 'w') as f:
        f.write("""
<!DOCTYPE html>
<html>
<head>
    <title>Simulation Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .button { padding: 10px 20px; background-color: #4CAF50; color: white; border: none; cursor: pointer; }
        .button:hover { background-color: #45a049; }
    </style>
</head>
<body>
    <h2>IoT Data Processing Simulation</h2>
    <form action="/run_simulation" method="post">
        <button type="submit" class="button">Run Simulation</button>
    </form>
</body>
</html>
        """)
    
    with open('templates/results.html', 'w') as f:
        f.write("""
<!DOCTYPE html>
<html>
<head>
    <title>Simulation Results</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        table { border-collapse: collapse; width: 80%; margin-top: 20px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        img { max-width: 100%; height: auto; margin-top: 20px; }
    </style>
</head>
<body>
    <h2>Simulation Results</h2>
    <a href="/"><button>Back</button></a>
    <table>
        <tr><th>Tenant</th><th>Raw Exec Time (ms)</th><th>Raw Memory</th><th>Framework Exec Time (ms)</th><th>Framework Memory</th></tr>
        {% for tenant_id in raw.keys() %}
        <tr>
            <td>T{{ tenant_id }}</td>
            <td>{{ raw[tenant_id].execution_time_ms|round(2) }}</td>
            <td>{{ raw[tenant_id].memory_used }}</td>
            <td>{{ framework[tenant_id].execution_time_ms|round(2) }}</td>
            <td>{{ framework[tenant_id].memory_used }}</td>
        </tr>
        {% endfor %}
    </table>
    <h3>Execution Time Plot</h3>
    <img src="{{ url_for('static', filename='execution_time.png') }}" alt="Execution Time">
    <h3>Memory Usage Plot</h3>
    <img src="{{ url_for('static', filename='memory_usage.png') }}" alt="Memory Usage">
</body>
</html>
        """)
    
   # app.run(host='0.0.0.0', port=8080)  # Cloud9 default port