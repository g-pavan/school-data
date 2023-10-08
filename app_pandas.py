from flask import Flask, request, jsonify, redirect, url_for, make_response
import pandas as pd

app = Flask(__name__)

# Create a global DataFrame to store your data
master_df = pd.DataFrame()

def read_data():
    global master_df
    # Read data from the CSV file
    data = pd.read_csv('master_data.csv')

    # Store the data in the master_df
    master_df = data

# Function to find top performers in a given subject
def find_nth_highest_students(master_df, subject, rank):
    result = master_df.groupby('school_name').apply(lambda x: x.nlargest(rank, subject).iloc[-1]).reset_index(drop=True)
    return result[['school_name', 'student_name', subject]]

# Function to find top performers in each subject
def find_top_performers(data_frame, subjects):
    top_performers = []

    for subject in subjects:
        idx_max = data_frame[data_frame[subject] == data_frame[subject].max()].index
        top_students_names = ', '.join(data_frame.loc[idx_max, 'student_name'])
        top_score = data_frame.loc[idx_max[0], subject]
        top_performers.append({'subject': subject, 'student_names': top_students_names, 'score': top_score})

    return pd.DataFrame(top_performers)


@app.route('/api/get_master_data', methods=['GET'])
def get_master_data():
    return jsonify({"data_frame" : master_df.to_dict(orient='records')})

@app.route('/api/schema_validate', methods=['GET'])
def read_and_validate_data():
    master_schema = {
        "school_id": "Int64",
        "school_name": "string",
        "student_id": "Int64",
        "student_name": "string",
        "telugu": "Int64",
        "hindi": "Int64",
        "english": "Int64",
        "maths": "Int64",
        "physics": "Int64",
        "total_marks": "Int64"
    }
    
    for col, dtype in master_schema.items():
        master_df[col] = master_df[col].astype(dtype, errors='ignore')

    return jsonify({"schema" : str(master_df.dtypes)})


@app.route('/api/get_topper_in_subject', methods=['POST'])
def get_topper_in_subject():
    subject = request.get_json().get("subject")

    result_df = find_nth_highest_students(master_df, subject, 1)

    return jsonify({"data_frame" : result_df.to_dict(orient='records')})

@app.route('/api/get_n_ranker_in_subject/<string:subject>/<int:n>', methods=['GET'])
def get_n_ranker_in_subject(subject, n):
    result_df = find_nth_highest_students(master_df, subject, n)
    return jsonify({"data_frame" : result_df.to_dict(orient='records')})

@app.route('/api/count_students', methods=['GET'])
def count_students():
    result_df = master_df.groupby("school_name")["student_id"].agg(count="count").reset_index() 
    return jsonify({"data_frame" : result_df.to_dict(orient='records')})
    

if __name__ == "__main__":
    read_data()
    app.run(debug=True)

    del master_df