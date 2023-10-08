import pandas as pd
from SelectStatement import SelectStatement

# Function to read and validate the master data
def read_and_validate_data(file_path):
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
    
    master_df = pd.read_csv(file_path)
    
    for col, dtype in master_schema.items():
        master_df[col] = master_df[col].astype(dtype, errors='ignore')
    
    return master_df

# Function to normalize data into separate DataFrames
def normalize_data(data_frame):
    return {
        "school_df": data_frame[["school_id", "school_name"]].drop_duplicates().set_index("school_id"),
        "school_student_df": data_frame[["school_id", "student_id"]],
        "students_df": data_frame[["student_id", "student_name", "telugu", "hindi", "english", "maths", "physics", "total_marks"]].set_index("student_id")
    }

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

# Main function
def main():
    file_path = "master_data.csv"
    
    # Read and validate the master data
    master_df = read_and_validate_data(file_path)
    master_df.dropna(inplace=True)
    # master_df.set_index("student_id", inplace=True)

    # # Normalize data
    normalized_data = normalize_data(master_df)
    school_student_df = normalized_data['school_student_df']
    school_df = normalized_data['school_df']
    students_df = normalized_data['students_df']
    # print(normalized_data["school_df"])
    # print(normalized_data["school_student_df"])
    # print(normalized_data["students_df"])

    # merged_school_student_df = school_student_df.merge(school_df, on='school_id', how='left')

    # merged_df = merged_school_student_df.merge(students_df, on='student_id', how='inner')
    # print(merged_df)
    # print(master_df)
    
    # Create a SelectStatement instance for df1
    select_query = SelectStatement(master_df)

    # print(select_query.select(["school_id", "student_name"]))

    # Build and execute a SELECT query
    print(select_query.select('student_name', 'total_marks').where((master_df["total_marks"] > 450) & (master_df["total_marks"] < 500)).execute())

    # print(select_query.join(school_df, on_columns='school_id').execute())

    # print(master_df[master_df["total_marks"] > 480][["student_name", "school_name"]].iloc[3:5])


    # # Query 1: Finding the top students who performed well in Telugu
    # top_performer = find_nth_highest_students(master_df, 'telugu', 15)
    # print(top_performer)

    # # Query 2: Count the number of students in each school
    # no_of_students = master_df.groupby("school_name")["student_id"].count()
    # print(no_of_students)

    # # Query 3: Top performer in each subject
    # result_performers = find_top_performers(master_df, ['telugu', 'hindi', 'english', 'maths', 'physics'])
    # print(result_performers)

    # # Query 4: Print the top scorer in each school
    # result_scorers = find_nth_highest_students(master_df, "total_marks", 1)
    # print(result_scorers)

if __name__ == "__main__":
    main()
